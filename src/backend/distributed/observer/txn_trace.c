/*-------------------------------------------------------------------------
 *
 * txn_trace.c
 *	  Traces which nodes, shard groups and tenants each distributed
 *	  transaction coordinated by this node touches, and how it commits.
 *
 * While a transaction runs, every successful task adds its node to a
 * backend-local trace. Tasks of statements that target a single shard also
 * add their shard group (colocation group + shard index, for
 * hash-distributed tables) and distribution column value; statements that
 * fan out to many shards are only counted, since touching every shard says
 * nothing about which shards belong together. When Citus finishes the transaction, the trace updates:
 *
 *	- summary counters (transactions, multi-node, multi-shard-group,
 *	  two-phase commits, aborts, prepare/commit time);
 *	- co-access counts for every pair of shard groups and every pair of
 *	  tenants in the same colocation group. These pairs are what a
 *	  placement decision should avoid splitting across nodes;
 *	- a "txn" event, for a sample of transactions (always for two-phase
 *	  commits and aborts).
 *
 * The end of the transaction runs inside Citus's commit callback, which
 * must not error or allocate much, so this file only uses fixed-size
 * buffers there.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdarg.h>

#include "fmgr.h"
#include "funcapi.h"

#include "common/pg_prng.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

#include "distributed/metadata_cache.h"
#include "distributed/observer/obs_ring.h"
#include "distributed/observer/observer.h"
#include "distributed/observer/observer_tables.h"
#include "distributed/tuplestore.h"

#define MAX_TRACE_NODES 16
#define MAX_TRACE_GROUPS 32
#define MAX_TRACE_TENANTS 16

typedef struct TraceGroup
{
	int32 colocationId;
	int32 shardIndex;
} TraceGroup;

typedef struct TraceTenant
{
	int32 colocationId;
	char value[OBS_TENANT_LEN];
} TraceTenant;

typedef struct TransactionTrace
{
	bool active;
	bool truncated;
	bool twoPhase;
	int fanOutTasks;
	TimestampTz startTime;
	uint64 prepareUs;
	uint64 commitUs;
	int nodeCount;
	int32 nodes[MAX_TRACE_NODES];
	int groupCount;
	TraceGroup groups[MAX_TRACE_GROUPS];
	int tenantCount;
	TraceTenant tenants[MAX_TRACE_TENANTS];
} TransactionTrace;

typedef struct ObsTxnSummary
{
	slock_t mutex;
	uint64 txns;
	uint64 multiNode;
	uint64 multiShardGroup;
	uint64 twoPhaseCommits;
	uint64 aborted;
	double prepareUs;
	double commitUs;
} ObsTxnSummary;

/* a JSON document built in a fixed buffer, never longer than an event */
typedef struct FixedJson
{
	char data[OBS_RING_PAYLOAD_LEN];
	int length;
	int reserve;                /* bytes kept free for closing the document */
	bool full;
} FixedJson;

static TransactionTrace Trace;
static ObsTxnSummary *SharedSummary = NULL;

static void ResetTrace(void);
static void UpdateSummary(bool committed);
static void CountPairs(void);
static void EmitEvent(bool committed, uint64 durationUs);
static void JsonAppend(FixedJson *json, const char *format,...) pg_attribute_printf(2, 3);
static void JsonAppendString(FixedJson *json, const char *value);

PG_FUNCTION_INFO_V1(citus_observer_txn_stats);
PG_FUNCTION_INFO_V1(citus_observer_shard_group_coaccess);
PG_FUNCTION_INFO_V1(citus_observer_tenant_coaccess);


/*
 * ObserverTxnSummaryShmemSize returns the shared memory the summary needs.
 */
Size
ObserverTxnSummaryShmemSize(void)
{
	return sizeof(ObsTxnSummary);
}


/*
 * ObserverTxnSummaryShmemInit sets up or attaches to the summary.
 */
void
ObserverTxnSummaryShmemInit(void *memory, bool found)
{
	SharedSummary = (ObsTxnSummary *) memory;
	if (!found)
	{
		memset(SharedSummary, 0, sizeof(ObsTxnSummary));
		SpinLockInit(&SharedSummary->mutex);
	}
}


/*
 * ObserverResetTxnSummary zeroes the summary counters.
 */
void
ObserverResetTxnSummary(void)
{
	if (SharedSummary == NULL)
	{
		return;
	}
	SpinLockAcquire(&SharedSummary->mutex);
	SharedSummary->txns = 0;
	SharedSummary->multiNode = 0;
	SharedSummary->multiShardGroup = 0;
	SharedSummary->twoPhaseCommits = 0;
	SharedSummary->aborted = 0;
	SharedSummary->prepareUs = 0;
	SharedSummary->commitUs = 0;
	SpinLockRelease(&SharedSummary->mutex);
}


/*
 * ResetTrace forgets the current transaction's trace.
 */
static void
ResetTrace(void)
{
	Trace.active = false;
	Trace.truncated = false;
	Trace.twoPhase = false;
	Trace.prepareUs = 0;
	Trace.commitUs = 0;
	Trace.fanOutTasks = 0;
	Trace.nodeCount = 0;
	Trace.groupCount = 0;
	Trace.tenantCount = 0;
}


/*
 * ObserverTraceTask adds a successfully executed task to the current
 * transaction's trace. targeted is whether its statement ran on a single
 * shard. Router modifications carry their distribution column value on the
 * statement rather than the task, so statementPartitionKeyValue is used
 * when the task has none.
 */
void
ObserverTraceTask(Task *task, int32 nodeId, bool targeted,
				  Const *statementPartitionKeyValue)
{
	if (!ObserverAreaEnabled(OBS_AREA_TXN))
	{
		return;
	}

	if (!Trace.active)
	{
		ResetTrace();
		Trace.active = true;
		Trace.startTime = GetCurrentTimestamp();
	}

	bool nodeSeen = false;
	for (int i = 0; i < Trace.nodeCount; i++)
	{
		if (Trace.nodes[i] == nodeId)
		{
			nodeSeen = true;
			break;
		}
	}
	if (!nodeSeen)
	{
		if (Trace.nodeCount < MAX_TRACE_NODES)
		{
			Trace.nodes[Trace.nodeCount++] = nodeId;
		}
		else
		{
			Trace.truncated = true;
		}
	}

	if (!targeted)
	{
		Trace.fanOutTasks++;
		return;
	}

	Oid relationId = InvalidOid;
	uint32 colocationId = 0;
	int shardIndex = 0;
	bool isHashDistributed = false;

	if (!LookupShardPosition(task->anchorShardId, &relationId, &colocationId,
							 &shardIndex, &isHashDistributed) || !isHashDistributed)
	{
		return;
	}

	bool groupSeen = false;
	for (int i = 0; i < Trace.groupCount; i++)
	{
		if (Trace.groups[i].colocationId == (int32) colocationId &&
			Trace.groups[i].shardIndex == shardIndex)
		{
			groupSeen = true;
			break;
		}
	}
	if (!groupSeen)
	{
		if (Trace.groupCount < MAX_TRACE_GROUPS)
		{
			Trace.groups[Trace.groupCount].colocationId = (int32) colocationId;
			Trace.groups[Trace.groupCount].shardIndex = shardIndex;
			Trace.groupCount++;
		}
		else
		{
			Trace.truncated = true;
		}
	}

	Const *partitionKeyValue = task->partitionKeyValue != NULL ?
							   task->partitionKeyValue : statementPartitionKeyValue;
	if (partitionKeyValue == NULL || partitionKeyValue->constisnull)
	{
		return;
	}

	Oid outputFunction = InvalidOid;
	bool isVarlena = false;

	getTypeOutputInfo(partitionKeyValue->consttype, &outputFunction, &isVarlena);
	char *valueText = OidOutputFunctionCall(outputFunction,
											partitionKeyValue->constvalue);

	for (int i = 0; i < Trace.tenantCount; i++)
	{
		if (Trace.tenants[i].colocationId == (int32) colocationId &&
			strncmp(Trace.tenants[i].value, valueText, OBS_TENANT_LEN - 1) == 0)
		{
			pfree(valueText);
			return;
		}
	}
	if (Trace.tenantCount < MAX_TRACE_TENANTS)
	{
		Trace.tenants[Trace.tenantCount].colocationId = (int32) colocationId;
		strlcpy(Trace.tenants[Trace.tenantCount].value, valueText, OBS_TENANT_LEN);
		Trace.tenantCount++;
	}
	else
	{
		Trace.truncated = true;
	}
	pfree(valueText);
}


/*
 * ObserverCommitPhase records the duration of preparing or committing the
 * remote transactions of the current distributed transaction.
 */
void
ObserverCommitPhase(ObsCommitPhase phase, uint64 durationUs, bool twoPhase)
{
	if (!Trace.active)
	{
		return;
	}

	if (twoPhase)
	{
		Trace.twoPhase = true;
	}
	if (phase == OBS_COMMIT_PHASE_PREPARE)
	{
		Trace.prepareUs += durationUs;
	}
	else
	{
		Trace.commitUs += durationUs;
	}
}


/*
 * ObserverDistributedTransactionEnd finalizes the current trace. Called by
 * Citus at the end of commit or abort processing.
 */
void
ObserverDistributedTransactionEnd(bool committed)
{
	if (!Trace.active)
	{
		return;
	}

	if (ObserverAreaEnabled(OBS_AREA_TXN))
	{
		TimestampTz now = GetCurrentTimestamp();
		uint64 durationUs = (uint64) Max(0, now - Trace.startTime);

		UpdateSummary(committed);
		if (committed)
		{
			CountPairs();
		}

		if (Trace.twoPhase || !committed || ObserverTxnEventSampleRate >= 1.0 ||
			pg_prng_double(&pg_global_prng_state) < ObserverTxnEventSampleRate)
		{
			EmitEvent(committed, durationUs);
		}
	}

	ResetTrace();
}


/*
 * UpdateSummary adds the trace to the summary counters.
 */
static void
UpdateSummary(bool committed)
{
	if (SharedSummary == NULL)
	{
		return;
	}

	SpinLockAcquire(&SharedSummary->mutex);
	SharedSummary->txns++;
	if (Trace.nodeCount > 1)
	{
		SharedSummary->multiNode++;
	}
	if (Trace.groupCount > 1)
	{
		SharedSummary->multiShardGroup++;
	}
	if (Trace.twoPhase && committed)
	{
		SharedSummary->twoPhaseCommits++;
	}
	if (!committed)
	{
		SharedSummary->aborted++;
	}
	SharedSummary->prepareUs += Trace.prepareUs;
	SharedSummary->commitUs += Trace.commitUs;
	SpinLockRelease(&SharedSummary->mutex);
}


/*
 * CountPairs increments the co-access count of every pair of shard groups
 * and of tenants in the same colocation group.
 */
static void
CountPairs(void)
{
	for (int i = 0; i < Trace.groupCount; i++)
	{
		for (int j = i + 1; j < Trace.groupCount; j++)
		{
			TraceGroup *a = &Trace.groups[i];
			TraceGroup *b = &Trace.groups[j];
			ObsGroupPairKey key;

			if (a->colocationId != b->colocationId)
			{
				continue;
			}

			memset(&key, 0, sizeof(key));
			key.colocationId = a->colocationId;
			key.shardIndexA = Min(a->shardIndex, b->shardIndex);
			key.shardIndexB = Max(a->shardIndex, b->shardIndex);

			ObsGroupPairEntry *entry = ObsTableLockEntry(OBS_TABLE_GROUP_COACCESS, &key,
														 NULL);
			if (entry == NULL)
			{
				continue;
			}
			entry->txns++;
			if (Trace.twoPhase)
			{
				entry->twoPhaseCommits++;
			}
			ObsTableUnlockEntry(OBS_TABLE_GROUP_COACCESS, entry);
		}
	}

	for (int i = 0; i < Trace.tenantCount; i++)
	{
		for (int j = i + 1; j < Trace.tenantCount; j++)
		{
			TraceTenant *a = &Trace.tenants[i];
			TraceTenant *b = &Trace.tenants[j];
			ObsTenantPairKey key;

			if (a->colocationId != b->colocationId)
			{
				continue;
			}

			bool aFirst = strcmp(a->value, b->value) < 0;

			memset(&key, 0, sizeof(key));
			key.colocationId = a->colocationId;
			strlcpy(key.tenantA, aFirst ? a->value : b->value, OBS_TENANT_LEN);
			strlcpy(key.tenantB, aFirst ? b->value : a->value, OBS_TENANT_LEN);

			ObsTenantPairEntry *entry = ObsTableLockEntry(OBS_TABLE_TENANT_COACCESS,
														  &key, NULL);
			if (entry == NULL)
			{
				continue;
			}
			entry->txns++;
			ObsTableUnlockEntry(OBS_TABLE_TENANT_COACCESS, entry);
		}
	}
}


/*
 * JsonAppend appends formatted text unless the document would no longer
 * fit in an event with json->reserve bytes left to close it.
 */
static void
JsonAppend(FixedJson *json, const char *format,...)
{
	char piece[256];
	va_list args;

	if (json->full)
	{
		return;
	}

	va_start(args, format);
	int pieceLength = vsnprintf(piece, sizeof(piece), format, args);
	va_end(args);

	if (pieceLength < 0 || pieceLength >= (int) sizeof(piece) ||
		json->length + pieceLength >= (int) sizeof(json->data) - json->reserve)
	{
		json->full = true;
		return;
	}

	memcpy(json->data + json->length, piece, pieceLength);
	json->length += pieceLength;
	json->data[json->length] = '\0';
}


/*
 * JsonAppendString appends a quoted, escaped JSON string.
 */
static void
JsonAppendString(FixedJson *json, const char *value)
{
	char escaped[OBS_TENANT_LEN * 2 + 3];
	int length = 0;

	escaped[length++] = '"';
	for (const char *c = value; *c != '\0' && length < (int) sizeof(escaped) - 3; c++)
	{
		if (*c == '"' || *c == '\\')
		{
			escaped[length++] = '\\';
			escaped[length++] = *c;
		}
		else if ((unsigned char) *c >= 0x20)
		{
			escaped[length++] = *c;
		}
	}
	escaped[length++] = '"';
	escaped[length] = '\0';
	JsonAppend(json, "%s", escaped);
}


/*
 * EmitEvent pushes a "txn" event describing the trace.
 */
static void
EmitEvent(bool committed, uint64 durationUs)
{
	FixedJson json = { .length = 0, .reserve = 200, .full = false };

	json.data[0] = '\0';
	JsonAppend(&json, "{\"duration_us\": " UINT64_FORMAT ", \"fan_out_tasks\": %d, "
			   "\"nodes\": [", durationUs, Trace.fanOutTasks);
	for (int i = 0; i < Trace.nodeCount; i++)
	{
		JsonAppend(&json, "%s%d", i > 0 ? ", " : "", Trace.nodes[i]);
	}
	JsonAppend(&json, "], \"groups\": [");
	for (int i = 0; i < Trace.groupCount; i++)
	{
		JsonAppend(&json, "%s{\"colocation_id\": %d, \"shard_index\": %d}",
				   i > 0 ? ", " : "", Trace.groups[i].colocationId,
				   Trace.groups[i].shardIndex);
	}
	JsonAppend(&json, "], \"tenants\": [");
	for (int i = 0; i < Trace.tenantCount; i++)
	{
		JsonAppend(&json, "%s{\"colocation_id\": %d, \"value\": ",
				   i > 0 ? ", " : "", Trace.tenants[i].colocationId);
		JsonAppendString(&json, Trace.tenants[i].value);
		JsonAppend(&json, "}");
	}

	/* the closing part always fits in the reserve */
	bool truncated = Trace.truncated || json.full;
	json.full = false;
	json.reserve = 0;
	JsonAppend(&json, "], \"two_pc\": %s, \"prepare_us\": " UINT64_FORMAT
			   ", \"commit_us\": " UINT64_FORMAT ", \"outcome\": \"%s\"%s}",
			   Trace.twoPhase ? "true" : "false", Trace.prepareUs, Trace.commitUs,
			   committed ? "commit" : "abort", truncated ? ", \"truncated\": true" : "");

	ObserverPushEvent("txn", json.data, json.length);
}


/*
 * citus_observer_txn_stats returns the summary counters.
 */
Datum
citus_observer_txn_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	Datum values[8];
	bool isNulls[8] = { false };
	ObsTxnSummary copy;

	if (SharedSummary == NULL)
	{
		PG_RETURN_VOID();
	}

	SpinLockAcquire(&SharedSummary->mutex);
	copy = *SharedSummary;
	SpinLockRelease(&SharedSummary->mutex);

	values[0] = Int64GetDatum((int64) copy.txns);
	values[1] = Int64GetDatum((int64) copy.multiNode);
	values[2] = Int64GetDatum((int64) copy.multiShardGroup);
	values[3] = Int64GetDatum((int64) copy.twoPhaseCommits);
	values[4] = Int64GetDatum((int64) copy.aborted);
	values[5] = Float8GetDatum(copy.prepareUs / 1000.0);
	values[6] = Float8GetDatum(copy.commitUs / 1000.0);
	values[7] = TimestampTzGetDatum(ObserverAreaResetTime(OBS_AREA_TXN));

	tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	PG_RETURN_VOID();
}


/*
 * citus_observer_shard_group_coaccess returns the shard group pair table.
 */
Datum
citus_observer_shard_group_coaccess(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsGroupPairEntry *entry = NULL;

	LWLockAcquire(ObsTableLock(OBS_TABLE_GROUP_COACCESS), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_GROUP_COACCESS));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsGroupPairEntry copy;
		Datum values[5];
		bool isNulls[5] = { false };

		SpinLockAcquire(&entry->mutex);
		copy = *entry;
		SpinLockRelease(&entry->mutex);

		values[0] = Int32GetDatum(copy.key.colocationId);
		values[1] = Int32GetDatum(copy.key.shardIndexA);
		values[2] = Int32GetDatum(copy.key.shardIndexB);
		values[3] = Int64GetDatum((int64) copy.txns);
		values[4] = Int64GetDatum((int64) copy.twoPhaseCommits);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_GROUP_COACCESS));

	PG_RETURN_VOID();
}


/*
 * citus_observer_tenant_coaccess returns the tenant pair table.
 */
Datum
citus_observer_tenant_coaccess(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsTenantPairEntry *entry = NULL;

	LWLockAcquire(ObsTableLock(OBS_TABLE_TENANT_COACCESS), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_TENANT_COACCESS));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsTenantPairEntry copy;
		Datum values[4];
		bool isNulls[4] = { false };

		SpinLockAcquire(&entry->mutex);
		copy = *entry;
		SpinLockRelease(&entry->mutex);

		values[0] = Int32GetDatum(copy.key.colocationId);
		values[1] = CStringGetTextDatum(copy.key.tenantA);
		values[2] = CStringGetTextDatum(copy.key.tenantB);
		values[3] = Int64GetDatum((int64) copy.txns);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_TENANT_COACCESS));

	PG_RETURN_VOID();
}
