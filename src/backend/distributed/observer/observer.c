/*-------------------------------------------------------------------------
 *
 * observer.c
 *	  Settings, shared memory and SQL-visible plumbing of the observer:
 *	  the shared hash tables, the event ring, status(), reset() and
 *	  events().
 *
 * Each area of instrumentation records into a shared hash table guarded by
 * one LWLock. Updating an existing entry takes the lock in shared mode plus
 * the entry's spinlock, so concurrent backends only serialize on the same
 * entry. Creating an entry takes the lock exclusively. Tables have a fixed
 * capacity (citus_observer.max_entries); once full, new keys are counted as
 * dropped rather than evicting old ones, so counters stay exact.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "lib/stringinfo.h"
#include "nodes/miscnodes.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"

#include "distributed/observer/obs_ring.h"
#include "distributed/observer/observer.h"
#include "distributed/observer/observer_tables.h"
#include "distributed/tuplestore.h"

#define OBSERVER_TRANCHE_NAME "citus_observer"
#define OBS_LOCK_RING OBS_TABLE_COUNT
#define OBS_LOCK_SKETCH (OBS_TABLE_COUNT + 1)
#define OBS_LOCK_COUNT (OBS_TABLE_COUNT + 2)

/* settings */
bool ObserverEnabled = true;
bool ObserverTrackTasks = true;
bool ObserverTrackResources = true;
bool ObserverTrackPlans = true;
bool ObserverTrackKeys = true;
bool ObserverTrackTransactions = true;
bool ObserverTrackContention = true;
bool ObserverTrackActions = true;
double ObserverKeySampleRate = 1.0;
double ObserverTxnEventSampleRate = 1.0;
int ObserverMaxEntries = 10000;
int ObserverEventSlots = 8192;
int ObserverContentionIntervalMs = 20;
char *ObserverContentionDatabase = NULL;

typedef struct ObsTableDef
{
	const char *name;
	Size keySize;
	Size entrySize;
	Size mutexOffset;
	int capacityFactor;
} ObsTableDef;

static const ObsTableDef ObsTableDefs[OBS_TABLE_COUNT] = {
	[OBS_TABLE_TASK] = {
		"citus_observer task stats", sizeof(ObsTaskKey), sizeof(ObsTaskEntry),
		offsetof(ObsTaskEntry, mutex), 1
	},
	[OBS_TABLE_RESOURCE] = {
		"citus_observer shard resource stats", sizeof(ObsResourceKey),
		sizeof(ObsResourceEntry), offsetof(ObsResourceEntry, mutex), 1
	},
	[OBS_TABLE_PLAN] = {
		"citus_observer query plans", sizeof(ObsPlanKey), sizeof(ObsPlanEntry),
		offsetof(ObsPlanEntry, mutex), 1
	},
	[OBS_TABLE_COLUMN] = {
		"citus_observer column usage", sizeof(ObsColumnKey), sizeof(ObsColumnEntry),
		offsetof(ObsColumnEntry, mutex), 4
	},
	[OBS_TABLE_JOIN] = {
		"citus_observer join edges", sizeof(ObsJoinKey), sizeof(ObsJoinEntry),
		offsetof(ObsJoinEntry, mutex), 2
	},
	[OBS_TABLE_GROUP_COACCESS] = {
		"citus_observer shard group coaccess", sizeof(ObsGroupPairKey),
		sizeof(ObsGroupPairEntry), offsetof(ObsGroupPairEntry, mutex), 1
	},
	[OBS_TABLE_TENANT_COACCESS] = {
		"citus_observer tenant coaccess", sizeof(ObsTenantPairKey),
		sizeof(ObsTenantPairEntry), offsetof(ObsTenantPairEntry, mutex), 1
	},
	[OBS_TABLE_CONTENTION] = {
		"citus_observer contention", sizeof(ObsContentionKey),
		sizeof(ObsContentionEntry), offsetof(ObsContentionEntry, mutex), 1
	},
};

static const char *const ObsAreaNames[OBS_AREA_COUNT] = {
	[OBS_AREA_TASK] = "task",
	[OBS_AREA_RESOURCE] = "resource",
	[OBS_AREA_PLAN] = "plan",
	[OBS_AREA_KEY] = "key",
	[OBS_AREA_TXN] = "txn",
	[OBS_AREA_CONTENTION] = "contention",
	[OBS_AREA_ACTION] = "action",
};

/* shared state that is not a hash table */
typedef struct ObsSharedState
{
	pg_atomic_uint64 dropped[OBS_TABLE_COUNT];
	TimestampTz areaReset[OBS_AREA_COUNT];
} ObsSharedState;

static ObsSharedState *SharedState = NULL;
static HTAB *SharedTables[OBS_TABLE_COUNT];
static LWLock *SharedLocks[OBS_LOCK_COUNT];
static ObsRing *SharedRing = NULL;

static shmem_request_hook_type PrevShmemRequestHook = NULL;
static shmem_startup_hook_type PrevShmemStartupHook = NULL;

static void RegisterObserverSettings(void);
static void ObserverShmemRequest(void);
static void ObserverShmemStartup(void);
static long TableCapacity(ObsTableId table);
static ObsTableId AreaTable(ObsArea area);
static void ResetArea(ObsArea area);

PG_FUNCTION_INFO_V1(citus_observer_status);
PG_FUNCTION_INFO_V1(citus_observer_reset);
PG_FUNCTION_INFO_V1(citus_observer_events);


/*
 * InitializeObserver registers the observer's settings, shared memory,
 * hooks and background worker. It is called from _PG_init after Citus has
 * installed its own hooks, so the observer's hooks wrap them.
 */
void
InitializeObserver(void)
{
	RegisterObserverSettings();

	PrevShmemRequestHook = shmem_request_hook;
	shmem_request_hook = ObserverShmemRequest;
	PrevShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = ObserverShmemStartup;

	InitializeObserverShardLookup();
	InitializeObserverExecutorHooks();
	RegisterObserverContentionWorker();
}


/*
 * RegisterObserverSettings defines the citus_observer.* settings.
 */
static void
RegisterObserverSettings(void)
{
	DefineCustomBoolVariable(
		"citus_observer.enabled",
		gettext_noop("Collects engine telemetry for the citus_observer extension."),
		gettext_noop("When off, nothing is recorded regardless of the per-area "
					 "settings."),
		&ObserverEnabled, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus_observer.track_tasks",
		gettext_noop("Records calls, time, rows and bytes of tasks sent to shard "
					 "placements."),
		NULL, &ObserverTrackTasks, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus_observer.track_resources",
		gettext_noop("Records CPU, buffer and WAL usage of statements on local "
					 "shards."),
		NULL, &ObserverTrackResources, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus_observer.track_plans",
		gettext_noop("Records how distributed queries are planned, why they are not "
					 "router queries and which columns they filter, join and group "
					 "on."),
		NULL, &ObserverTrackPlans, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus_observer.track_keys",
		gettext_noop("Tracks the most frequently accessed primary key values of "
					 "local shards."),
		NULL, &ObserverTrackKeys, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus_observer.track_transactions",
		gettext_noop("Traces the shard groups, tenants and nodes each distributed "
					 "transaction touches, and its commit protocol."),
		NULL, &ObserverTrackTransactions, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus_observer.track_contention",
		gettext_noop("Samples lock waits on local shards in a background worker."),
		NULL, &ObserverTrackContention, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"citus_observer.track_actions",
		gettext_noop("Records phase timings of shard moves, copies and splits."),
		NULL, &ObserverTrackActions, true, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable(
		"citus_observer.key_sample_rate",
		gettext_noop("Fraction of primary key lookups fed to the hot key sketch."),
		NULL, &ObserverKeySampleRate, 1.0, 0.0, 1.0, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable(
		"citus_observer.txn_event_sample_rate",
		gettext_noop("Fraction of distributed transactions that emit a txn event."),
		gettext_noop("Transactions that use two-phase commit or abort always emit "
					 "an event. Counters always include every transaction."),
		&ObserverTxnEventSampleRate, 1.0, 0.0, 1.0, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus_observer.max_entries",
		gettext_noop("Capacity of each observer statistics table."),
		NULL, &ObserverMaxEntries, 10000, 100, 1000000, PGC_POSTMASTER, 0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus_observer.event_slots",
		gettext_noop("Number of events kept in the observer's event ring."),
		NULL, &ObserverEventSlots, 8192, 64, 1000000, PGC_POSTMASTER, 0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"citus_observer.contention_interval_ms",
		gettext_noop("How often the contention sampler inspects lock waits."),
		NULL, &ObserverContentionIntervalMs, 20, 1, 10000, PGC_SIGHUP,
		GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomStringVariable(
		"citus_observer.contention_database",
		gettext_noop("Database the contention sampler connects to."),
		NULL, &ObserverContentionDatabase, "postgres", PGC_POSTMASTER, 0,
		NULL, NULL, NULL);

	MarkGUCPrefixReserved("citus_observer");
}


/*
 * TableCapacity returns how many entries a shared table may hold.
 */
static long
TableCapacity(ObsTableId table)
{
	return (long) ObserverMaxEntries * ObsTableDefs[table].capacityFactor;
}


/*
 * ObserverShmemRequest reserves shared memory and locks for the observer.
 */
static void
ObserverShmemRequest(void)
{
	if (PrevShmemRequestHook != NULL)
	{
		PrevShmemRequestHook();
	}

	Size size = MAXALIGN(sizeof(ObsSharedState));

	for (int table = 0; table < OBS_TABLE_COUNT; table++)
	{
		size = add_size(size, hash_estimate_size(TableCapacity(table),
												 ObsTableDefs[table].entrySize));
	}
	size = add_size(size, MAXALIGN(obs_ring_size(ObserverEventSlots)));
	size = add_size(size, MAXALIGN(ObserverKeySketchShmemSize()));
	size = add_size(size, MAXALIGN(ObserverTxnSummaryShmemSize()));

	RequestAddinShmemSpace(size);
	RequestNamedLWLockTranche(OBSERVER_TRANCHE_NAME, OBS_LOCK_COUNT);
}


/*
 * ObserverShmemStartup attaches to (and in the postmaster, creates) the
 * observer's shared memory.
 */
static void
ObserverShmemStartup(void)
{
	bool found = false;

	if (PrevShmemStartupHook != NULL)
	{
		PrevShmemStartupHook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	LWLockPadded *locks = GetNamedLWLockTranche(OBSERVER_TRANCHE_NAME);
	for (int lockIndex = 0; lockIndex < OBS_LOCK_COUNT; lockIndex++)
	{
		SharedLocks[lockIndex] = &locks[lockIndex].lock;
	}

	SharedState = ShmemInitStruct("citus_observer state", sizeof(ObsSharedState),
								  &found);
	if (!found)
	{
		for (int table = 0; table < OBS_TABLE_COUNT; table++)
		{
			pg_atomic_init_u64(&SharedState->dropped[table], 0);
		}
		TimestampTz now = GetCurrentTimestamp();
		for (int area = 0; area < OBS_AREA_COUNT; area++)
		{
			SharedState->areaReset[area] = now;
		}
	}

	for (int table = 0; table < OBS_TABLE_COUNT; table++)
	{
		HASHCTL info;

		memset(&info, 0, sizeof(info));
		info.keysize = ObsTableDefs[table].keySize;
		info.entrysize = ObsTableDefs[table].entrySize;
		SharedTables[table] = ShmemInitHash(ObsTableDefs[table].name,
											TableCapacity(table),
											TableCapacity(table),
											&info, HASH_ELEM | HASH_BLOBS);
	}

	SharedRing = ShmemInitStruct("citus_observer events",
								 obs_ring_size(ObserverEventSlots), &found);
	if (!found)
	{
		obs_ring_init(SharedRing, ObserverEventSlots);
	}

	void *sketch = ShmemInitStruct("citus_observer hot keys",
								   ObserverKeySketchShmemSize(), &found);
	ObserverKeySketchShmemInit(sketch, found);

	void *txnSummary = ShmemInitStruct("citus_observer txn summary",
									   ObserverTxnSummaryShmemSize(), &found);
	ObserverTxnSummaryShmemInit(txnSummary, found);

	LWLockRelease(AddinShmemInitLock);
}


/*
 * ObserverAreaEnabled returns whether an area should record right now.
 */
bool
ObserverAreaEnabled(ObsArea area)
{
	if (!ObserverEnabled || SharedState == NULL)
	{
		return false;
	}

	switch (area)
	{
		case OBS_AREA_TASK:
		{
			return ObserverTrackTasks;
		}

		case OBS_AREA_RESOURCE:
		{
			return ObserverTrackResources;
		}

		case OBS_AREA_PLAN:
		{
			return ObserverTrackPlans;
		}

		case OBS_AREA_KEY:
		{
			return ObserverTrackKeys;
		}

		case OBS_AREA_TXN:
		{
			return ObserverTrackTransactions;
		}

		case OBS_AREA_CONTENTION:
		{
			return ObserverTrackContention;
		}

		case OBS_AREA_ACTION:
		{
			return ObserverTrackActions;
		}

		default:
		{
			return false;
		}
	}
}


/*
 * ObsTableLockEntry finds or creates the entry for key and returns it with
 * the table lock and the entry's spinlock held. The caller updates the
 * entry's counters (without anything that can error or allocate) and then
 * calls ObsTableUnlockEntry(). Returns NULL, with nothing locked, when the
 * table is full and the key is new.
 */
void *
ObsTableLockEntry(ObsTableId table, const void *key, bool *created)
{
	HTAB *hash = SharedTables[table];
	LWLock *lock = SharedLocks[table];
	const ObsTableDef *def = &ObsTableDefs[table];

	if (created != NULL)
	{
		*created = false;
	}

	LWLockAcquire(lock, LW_SHARED);
	void *entry = hash_search(hash, key, HASH_FIND, NULL);

	if (entry == NULL)
	{
		bool found = false;

		LWLockRelease(lock);
		LWLockAcquire(lock, LW_EXCLUSIVE);

		entry = hash_search(hash, key, HASH_FIND, NULL);
		if (entry == NULL)
		{
			if (hash_get_num_entries(hash) >= TableCapacity(table))
			{
				pg_atomic_fetch_add_u64(&SharedState->dropped[table], 1);
				LWLockRelease(lock);
				return NULL;
			}

			entry = hash_search(hash, key, HASH_ENTER_NULL, &found);
			if (entry == NULL)
			{
				pg_atomic_fetch_add_u64(&SharedState->dropped[table], 1);
				LWLockRelease(lock);
				return NULL;
			}

			if (!found)
			{
				memset((char *) entry + def->keySize, 0, def->entrySize - def->keySize);
				SpinLockInit((slock_t *) ((char *) entry + def->mutexOffset));
				if (created != NULL)
				{
					*created = true;
				}
			}
		}
	}

	SpinLockAcquire((slock_t *) ((char *) entry + def->mutexOffset));
	return entry;
}


/*
 * ObsTableUnlockEntry releases the locks taken by ObsTableLockEntry().
 */
void
ObsTableUnlockEntry(ObsTableId table, void *entry)
{
	SpinLockRelease((slock_t *) ((char *) entry + ObsTableDefs[table].mutexOffset));
	LWLockRelease(SharedLocks[table]);
}


/*
 * ObsTableHash returns a shared table for readers, which must hold
 * ObsTableLock() and take each entry's spinlock while copying it.
 */
HTAB *
ObsTableHash(ObsTableId table)
{
	return SharedTables[table];
}


/*
 * ObsTableLock returns the lock guarding a shared table.
 */
LWLock *
ObsTableLock(ObsTableId table)
{
	return SharedLocks[table];
}


/*
 * ObserverKeySketchLock returns the lock guarding the hot key sketch's
 * top-K table.
 */
LWLock *
ObserverKeySketchLock(void)
{
	return SharedLocks[OBS_LOCK_SKETCH];
}


/*
 * ObsTableReset removes every entry of a shared table.
 */
void
ObsTableReset(ObsTableId table)
{
	HASH_SEQ_STATUS status;
	void *entry = NULL;

	LWLockAcquire(SharedLocks[table], LW_EXCLUSIVE);
	hash_seq_init(&status, SharedTables[table]);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		hash_search(SharedTables[table], entry, HASH_REMOVE, NULL);
	}
	pg_atomic_write_u64(&SharedState->dropped[table], 0);
	LWLockRelease(SharedLocks[table]);
}


/*
 * ObserverAreaResetTime returns when an area was last reset.
 */
TimestampTz
ObserverAreaResetTime(ObsArea area)
{
	return SharedState->areaReset[area];
}


/*
 * ObserverSetAreaResetTime marks an area as reset now.
 */
void
ObserverSetAreaResetTime(ObsArea area)
{
	SharedState->areaReset[area] = GetCurrentTimestamp();
}


/*
 * ObserverPushEvent appends a JSON object to the event ring.
 */
void
ObserverPushEvent(const char *kind, const char *payload, int length)
{
	if (SharedRing == NULL)
	{
		return;
	}

	TimestampTz now = GetCurrentTimestamp();

	LWLockAcquire(SharedLocks[OBS_LOCK_RING], LW_EXCLUSIVE);
	obs_ring_push(SharedRing, now, kind, payload, length);
	LWLockRelease(SharedLocks[OBS_LOCK_RING]);
}


/*
 * ObsEscapeJsonString appends str to buf as a quoted JSON string.
 */
void
ObsEscapeJsonString(StringInfo buf, const char *str)
{
	if (str == NULL)
	{
		appendStringInfoString(buf, "null");
		return;
	}
	escape_json(buf, str);
}


/*
 * AreaTable returns the shared table that represents an area in status().
 */
static ObsTableId
AreaTable(ObsArea area)
{
	switch (area)
	{
		case OBS_AREA_TASK:
		{
			return OBS_TABLE_TASK;
		}

		case OBS_AREA_RESOURCE:
		{
			return OBS_TABLE_RESOURCE;
		}

		case OBS_AREA_PLAN:
		{
			return OBS_TABLE_PLAN;
		}

		case OBS_AREA_TXN:
		{
			return OBS_TABLE_GROUP_COACCESS;
		}

		case OBS_AREA_CONTENTION:
		{
			return OBS_TABLE_CONTENTION;
		}

		default:
		{
			return OBS_TABLE_COUNT;
		}
	}
}


/*
 * ResetArea clears every table that belongs to an area.
 */
static void
ResetArea(ObsArea area)
{
	switch (area)
	{
		case OBS_AREA_TASK:
		{
			ObsTableReset(OBS_TABLE_TASK);
			break;
		}

		case OBS_AREA_RESOURCE:
		{
			ObsTableReset(OBS_TABLE_RESOURCE);
			break;
		}

		case OBS_AREA_PLAN:
		{
			ObsTableReset(OBS_TABLE_PLAN);
			ObsTableReset(OBS_TABLE_COLUMN);
			ObsTableReset(OBS_TABLE_JOIN);
			break;
		}

		case OBS_AREA_KEY:
		{
			ObserverResetKeySketch();
			break;
		}

		case OBS_AREA_TXN:
		{
			ObserverResetTxnSummary();
			ObsTableReset(OBS_TABLE_GROUP_COACCESS);
			ObsTableReset(OBS_TABLE_TENANT_COACCESS);
			break;
		}

		case OBS_AREA_CONTENTION:
		{
			ObsTableReset(OBS_TABLE_CONTENTION);
			break;
		}

		default:
		{
			/* actions only produce events, which readers consume by seq */
			break;
		}
	}
	ObserverSetAreaResetTime(area);
}


/*
 * citus_observer_status reports, per area, whether it records, how full
 * its table is and how many new keys were dropped because it was full.
 */
Datum
citus_observer_status(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	if (SharedState == NULL)
	{
		ereport(ERROR, (errmsg("citus_observer shared memory is not initialized"),
						errhint("citus must be in shared_preload_libraries.")));
	}

	for (int area = 0; area < OBS_AREA_COUNT; area++)
	{
		Datum values[6];
		bool isNulls[6] = { false };
		ObsTableId table = AreaTable(area);
		int64 entries = 0;
		int64 capacity = 0;
		int64 dropped = 0;

		if (table < OBS_TABLE_COUNT)
		{
			LWLockAcquire(SharedLocks[table], LW_SHARED);
			entries = hash_get_num_entries(SharedTables[table]);
			LWLockRelease(SharedLocks[table]);
			capacity = TableCapacity(table);
			dropped = pg_atomic_read_u64(&SharedState->dropped[table]);
		}
		else if (area == OBS_AREA_ACTION)
		{
			LWLockAcquire(SharedLocks[OBS_LOCK_RING], LW_SHARED);
			uint64 head = obs_ring_head(SharedRing);
			uint64 oldest = obs_ring_oldest(SharedRing);
			LWLockRelease(SharedLocks[OBS_LOCK_RING]);
			entries = head == 0 ? 0 : (int64) (head - oldest + 1);
			capacity = SharedRing->nslots;
		}
		else if (area == OBS_AREA_KEY)
		{
			ObserverKeySketchUsage(&entries, &capacity);
		}

		values[0] = CStringGetTextDatum(ObsAreaNames[area]);
		values[1] = BoolGetDatum(ObserverAreaEnabled(area));
		values[2] = Int64GetDatum(entries);
		values[3] = Int64GetDatum(capacity);
		values[4] = Int64GetDatum(dropped);
		values[5] = TimestampTzGetDatum(SharedState->areaReset[area]);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}


/*
 * citus_observer_reset clears one area, or all of them when the argument
 * is NULL.
 */
Datum
citus_observer_reset(PG_FUNCTION_ARGS)
{
	if (SharedState == NULL)
	{
		ereport(ERROR, (errmsg("citus_observer shared memory is not initialized")));
	}

	if (PG_ARGISNULL(0))
	{
		for (int area = 0; area < OBS_AREA_COUNT; area++)
		{
			ResetArea(area);
		}
		PG_RETURN_VOID();
	}

	char *areaName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	for (int area = 0; area < OBS_AREA_COUNT; area++)
	{
		if (strcmp(areaName, ObsAreaNames[area]) == 0)
		{
			ResetArea(area);
			PG_RETURN_VOID();
		}
	}

	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unknown citus_observer area \"%s\"", areaName),
					errhint("Use one of task, resource, plan, key, txn, contention, "
							"action, or NULL for all.")));
}


/*
 * citus_observer_events returns events with seq > since, oldest first. If
 * events after since were overwritten before being read, a single "gap"
 * row reports how many were lost.
 */
Datum
citus_observer_events(PG_FUNCTION_ARGS)
{
	int64 since = PG_GETARG_INT64(0);
	int32 maxEvents = PG_GETARG_INT32(1);
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	if (SharedRing == NULL)
	{
		ereport(ERROR, (errmsg("citus_observer shared memory is not initialized")));
	}
	if (since < 0)
	{
		since = 0;
	}
	if (maxEvents <= 0)
	{
		PG_RETURN_VOID();
	}

	uint64 lost = 0;
	int copied = 0;
	ObsRingSlot *slots = palloc(sizeof(ObsRingSlot) * Min(maxEvents, SharedRing->nslots));

	LWLockAcquire(SharedLocks[OBS_LOCK_RING], LW_SHARED);
	uint64 seq = obs_ring_next(SharedRing, (uint64) since, &lost);
	uint64 head = obs_ring_head(SharedRing);
	for (; seq != 0 && seq <= head && copied < maxEvents &&
		 copied < (int) SharedRing->nslots; seq++)
	{
		const ObsRingSlot *slot = obs_ring_get(SharedRing, seq);
		if (slot != NULL)
		{
			slots[copied++] = *slot;
		}
	}
	LWLockRelease(SharedLocks[OBS_LOCK_RING]);

	if (lost > 0)
	{
		Datum values[4];
		bool isNulls[4] = { false };
		char payload[64];

		snprintf(payload, sizeof(payload), "{\"lost\": " UINT64_FORMAT "}", lost);
		values[0] = Int64GetDatum(since + (int64) lost);
		values[1] = TimestampTzGetDatum(GetCurrentTimestamp());
		values[2] = CStringGetTextDatum("gap");
		values[3] = DirectFunctionCall1(jsonb_in, CStringGetDatum(payload));
		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	for (int i = 0; i < copied; i++)
	{
		Datum values[4];
		bool isNulls[4] = { false };

		values[0] = Int64GetDatum((int64) slots[i].seq);
		values[1] = TimestampTzGetDatum((TimestampTz) slots[i].ts_us);
		values[2] = CStringGetTextDatum(slots[i].kind);

		/* a truncated payload is not valid JSON; report it as a string */
		ErrorSaveContext escontext = { T_ErrorSaveContext };
		if (!DirectInputFunctionCallSafe(jsonb_in, slots[i].payload, InvalidOid, -1,
										 (Node *) &escontext, &values[3]))
		{
			StringInfoData wrapped;

			initStringInfo(&wrapped);
			appendStringInfoString(&wrapped, "{\"truncated\": ");
			escape_json(&wrapped, slots[i].payload);
			appendStringInfoChar(&wrapped, '}');
			values[3] = DirectFunctionCall1(jsonb_in, CStringGetDatum(wrapped.data));
		}

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}
