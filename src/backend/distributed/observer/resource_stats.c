/*-------------------------------------------------------------------------
 *
 * resource_stats.c
 *	  CPU time, buffer and WAL usage of statements that run against local
 *	  shard relations, accumulated per shard.
 *
 * The executor hooks snapshot getrusage(), pgBufferUsage and pgWalUsage
 * when a top-level statement starts and charge the difference to the
 * shards in its range table when it ends. A statement touching several
 * shards splits its cost evenly. COPY into or out of a shard is handled by
 * the utility hook the same way. Nested statements (e.g. inside functions)
 * are included in their top-level statement.
 *
 * The executor start hook also feeds the hot key sketch, since that is
 * where index lookups on shards and their bound values are visible.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/resource.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/observer/observer.h"
#include "distributed/observer/observer_tables.h"
#include "distributed/tuplestore.h"

#define MAX_TRACKED_SHARDS 64

typedef struct ResourceSnapshot
{
	QueryDesc *queryDesc;       /* NULL for utility statements */
	instr_time startTime;
	struct rusage startUsage;
	BufferUsage startBuffers;
	WalUsage startWal;
	bool isWrite;
	int shardCount;
	uint64 shardIds[MAX_TRACKED_SHARDS];
} ResourceSnapshot;

static ExecutorStart_hook_type PrevExecutorStart = NULL;
static ExecutorRun_hook_type PrevExecutorRun = NULL;
static ExecutorFinish_hook_type PrevExecutorFinish = NULL;
static ExecutorEnd_hook_type PrevExecutorEnd = NULL;
static ProcessUtility_hook_type PrevProcessUtility = NULL;

/* how deep we are inside ExecutorRun/ExecutorFinish/ProcessUtility */
static int NestingLevel = 0;

/* snapshots of running top-level statements, allocated in TopMemoryContext */
static List *ActiveSnapshots = NIL;

static void ObserverExecutorStart(QueryDesc *queryDesc, int eflags);
static void ObserverExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
								uint64 count);
static void ObserverExecutorFinish(QueryDesc *queryDesc);
static void ObserverExecutorEnd(QueryDesc *queryDesc);
static void ObserverProcessUtility(PlannedStmt *pstmt, const char *queryString,
								   bool readOnlyTree, ProcessUtilityContext context,
								   ParamListInfo params, QueryEnvironment *queryEnv,
								   DestReceiver *dest, QueryCompletion *qc);
static void ObserverResourceXactCallback(XactEvent event, void *arg);
static ResourceSnapshot * StartSnapshot(QueryDesc *queryDesc, List *rangeTable,
										bool isWrite);
static void AddShard(ResourceSnapshot *snapshot, Oid relationId);
static void FinishSnapshot(ResourceSnapshot *snapshot, uint64 rows);
static double TimevalDiffMs(struct timeval end, struct timeval start);

PG_FUNCTION_INFO_V1(citus_observer_shard_resource_stats);


/*
 * InitializeObserverExecutorHooks wraps the executor and utility hooks
 * Citus installed.
 */
void
InitializeObserverExecutorHooks(void)
{
	PrevExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = ObserverExecutorStart;
	PrevExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = ObserverExecutorRun;
	PrevExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = ObserverExecutorFinish;
	PrevExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = ObserverExecutorEnd;
	PrevProcessUtility = ProcessUtility_hook != NULL ? ProcessUtility_hook :
						 standard_ProcessUtility;
	ProcessUtility_hook = ObserverProcessUtility;

	RegisterXactCallback(ObserverResourceXactCallback, NULL);
}


/*
 * ObserverResourceXactCallback drops snapshots of statements that never
 * reached ExecutorEnd because their transaction aborted.
 */
static void
ObserverResourceXactCallback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
	{
		list_free_deep(ActiveSnapshots);
		ActiveSnapshots = NIL;
		NestingLevel = 0;
	}
}


/*
 * StartSnapshot records the starting resource counters of a statement if it
 * touches at least one local shard.
 */
static ResourceSnapshot *
StartSnapshot(QueryDesc *queryDesc, List *rangeTable, bool isWrite)
{
	ResourceSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_declared_ptr(rangeTableEntry, rangeTable)
	{
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			AddShard(&snapshot, rangeTableEntry->relid);
		}
	}

	if (snapshot.shardCount == 0)
	{
		return NULL;
	}

	MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);
	ResourceSnapshot *saved = palloc(sizeof(ResourceSnapshot));
	MemoryContextSwitchTo(oldContext);

	*saved = snapshot;
	saved->queryDesc = queryDesc;
	saved->isWrite = isWrite;
	INSTR_TIME_SET_CURRENT(saved->startTime);
	getrusage(RUSAGE_SELF, &saved->startUsage);
	saved->startBuffers = pgBufferUsage;
	saved->startWal = pgWalUsage;
	return saved;
}


/*
 * AddShard adds the shard stored in relationId, if any, to a snapshot.
 */
static void
AddShard(ResourceSnapshot *snapshot, Oid relationId)
{
	uint64 shardId = ObserverShardIdForRelation(relationId);

	if (shardId == INVALID_SHARD_ID)
	{
		return;
	}

	for (int i = 0; i < snapshot->shardCount; i++)
	{
		if (snapshot->shardIds[i] == shardId)
		{
			return;
		}
	}

	if (snapshot->shardCount < MAX_TRACKED_SHARDS)
	{
		snapshot->shardIds[snapshot->shardCount++] = shardId;
	}
}


/*
 * TimevalDiffMs returns end - start in milliseconds.
 */
static double
TimevalDiffMs(struct timeval end, struct timeval start)
{
	return (double) (end.tv_sec - start.tv_sec) * 1000.0 +
		   (double) (end.tv_usec - start.tv_usec) / 1000.0;
}


/*
 * FinishSnapshot charges the resources used since the snapshot started to
 * its shards and frees it.
 */
static void
FinishSnapshot(ResourceSnapshot *snapshot, uint64 rows)
{
	instr_time endTime;
	struct rusage endUsage;
	BufferUsage buffers;
	WalUsage wal;

	INSTR_TIME_SET_CURRENT(endTime);
	getrusage(RUSAGE_SELF, &endUsage);

	memset(&buffers, 0, sizeof(buffers));
	BufferUsageAccumDiff(&buffers, &pgBufferUsage, &snapshot->startBuffers);
	memset(&wal, 0, sizeof(wal));
	WalUsageAccumDiff(&wal, &pgWalUsage, &snapshot->startWal);

	INSTR_TIME_SUBTRACT(endTime, snapshot->startTime);

	double share = 1.0 / snapshot->shardCount;
	double execMs = INSTR_TIME_GET_MILLISEC(endTime) * share;
	double userMs = TimevalDiffMs(endUsage.ru_utime, snapshot->startUsage.ru_utime) *
					share;
	double sysMs = TimevalDiffMs(endUsage.ru_stime, snapshot->startUsage.ru_stime) *
				   share;

	for (int i = 0; i < snapshot->shardCount; i++)
	{
		ObsResourceKey key;

		memset(&key, 0, sizeof(key));
		key.shardId = snapshot->shardIds[i];

		ObsResourceEntry *entry = ObsTableLockEntry(OBS_TABLE_RESOURCE, &key, NULL);
		if (entry == NULL)
		{
			continue;
		}

		entry->calls++;
		if (snapshot->isWrite)
		{
			entry->writeCalls++;
		}
		entry->execMs += execMs;
		entry->userMs += userMs;
		entry->sysMs += sysMs;
		entry->sharedBlksHit += buffers.shared_blks_hit * share;
		entry->sharedBlksRead += buffers.shared_blks_read * share;
		entry->sharedBlksDirtied += buffers.shared_blks_dirtied * share;
		entry->sharedBlksWritten += buffers.shared_blks_written * share;
		entry->tempBlksWritten += buffers.temp_blks_written * share;
		entry->walRecords += wal.wal_records * share;
		entry->walBytes += wal.wal_bytes * share;
		entry->rows += rows * share;

		ObsTableUnlockEntry(OBS_TABLE_RESOURCE, entry);
	}

	ActiveSnapshots = list_delete_ptr(ActiveSnapshots, snapshot);
	pfree(snapshot);
}


/*
 * ObserverExecutorStart starts a resource snapshot for top-level
 * statements on shards and feeds index lookups to the hot key sketch.
 */
static void
ObserverExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (PrevExecutorStart != NULL)
	{
		PrevExecutorStart(queryDesc, eflags);
	}
	else
	{
		standard_ExecutorStart(queryDesc, eflags);
	}

	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) || queryDesc->plannedstmt == NULL)
	{
		return;
	}

	if (NestingLevel == 0 && ObserverAreaEnabled(OBS_AREA_RESOURCE))
	{
		PlannedStmt *plannedStmt = queryDesc->plannedstmt;
		bool isWrite = plannedStmt->commandType != CMD_SELECT ||
					   plannedStmt->hasModifyingCTE || plannedStmt->rowMarks != NIL;

		ResourceSnapshot *snapshot = StartSnapshot(queryDesc, plannedStmt->rtable,
												   isWrite);
		if (snapshot != NULL)
		{
			MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);
			ActiveSnapshots = lappend(ActiveSnapshots, snapshot);
			MemoryContextSwitchTo(oldContext);
		}
	}

	if (ObserverAreaEnabled(OBS_AREA_KEY))
	{
		ObserverRecordKeyAccesses(queryDesc);
	}
}


/*
 * ObserverExecutorRun tracks nesting so that statements started while
 * another one runs are not counted twice.
 */
static void
ObserverExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	NestingLevel++;
	PG_TRY();
	{
		if (PrevExecutorRun != NULL)
		{
			PrevExecutorRun(queryDesc, direction, count);
		}
		else
		{
			standard_ExecutorRun(queryDesc, direction, count);
		}
	}
	PG_FINALLY();
	{
		NestingLevel--;
	}
	PG_END_TRY();
}


/*
 * ObserverExecutorFinish tracks nesting like ObserverExecutorRun.
 */
static void
ObserverExecutorFinish(QueryDesc *queryDesc)
{
	NestingLevel++;
	PG_TRY();
	{
		if (PrevExecutorFinish != NULL)
		{
			PrevExecutorFinish(queryDesc);
		}
		else
		{
			standard_ExecutorFinish(queryDesc);
		}
	}
	PG_FINALLY();
	{
		NestingLevel--;
	}
	PG_END_TRY();
}


/*
 * ObserverExecutorEnd charges a finished top-level statement's resources
 * to its shards.
 */
static void
ObserverExecutorEnd(QueryDesc *queryDesc)
{
	ResourceSnapshot *snapshot = NULL;
	foreach_declared_ptr(snapshot, ActiveSnapshots)
	{
		if (snapshot->queryDesc == queryDesc)
		{
			uint64 rows = queryDesc->estate != NULL ? queryDesc->estate->es_processed : 0;
			FinishSnapshot(snapshot, rows);
			break;
		}
	}

	if (PrevExecutorEnd != NULL)
	{
		PrevExecutorEnd(queryDesc);
	}
	else
	{
		standard_ExecutorEnd(queryDesc);
	}
}


/*
 * ObserverProcessUtility accounts COPY statements on shards, which do not
 * go through the executor, and tracks nesting for everything else.
 */
static void
ObserverProcessUtility(PlannedStmt *pstmt, const char *queryString,
					   bool readOnlyTree, ProcessUtilityContext context,
					   ParamListInfo params, QueryEnvironment *queryEnv,
					   DestReceiver *dest, QueryCompletion *qc)
{
	ResourceSnapshot *snapshot = NULL;
	Node *parsetree = pstmt->utilityStmt;

	if (NestingLevel == 0 && IsA(parsetree, CopyStmt) &&
		((CopyStmt *) parsetree)->relation != NULL &&
		ObserverAreaEnabled(OBS_AREA_RESOURCE))
	{
		CopyStmt *copyStatement = (CopyStmt *) parsetree;
		Oid relationId = RangeVarGetRelid(copyStatement->relation, NoLock, true);

		if (OidIsValid(relationId))
		{
			RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);

			rangeTableEntry->rtekind = RTE_RELATION;
			rangeTableEntry->relid = relationId;
			snapshot = StartSnapshot(NULL, list_make1(rangeTableEntry),
									 copyStatement->is_from);
			if (snapshot != NULL)
			{
				MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);
				ActiveSnapshots = lappend(ActiveSnapshots, snapshot);
				MemoryContextSwitchTo(oldContext);
			}
		}
	}

	NestingLevel++;
	PG_TRY();
	{
		PrevProcessUtility(pstmt, queryString, readOnlyTree, context, params,
						   queryEnv, dest, qc);
	}
	PG_FINALLY();
	{
		NestingLevel--;
	}
	PG_END_TRY();

	if (snapshot != NULL)
	{
		FinishSnapshot(snapshot, qc != NULL ? qc->nprocessed : 0);
	}
}


/*
 * citus_observer_shard_resource_stats returns the per-shard resource table.
 */
Datum
citus_observer_shard_resource_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsResourceEntry *entry = NULL;

	LWLockAcquire(ObsTableLock(OBS_TABLE_RESOURCE), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_RESOURCE));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsResourceEntry copy;
		Datum values[14];
		bool isNulls[14] = { false };

		SpinLockAcquire(&entry->mutex);
		copy = *entry;
		SpinLockRelease(&entry->mutex);

		values[0] = Int64GetDatum((int64) copy.key.shardId);
		values[1] = Int64GetDatum((int64) copy.calls);
		values[2] = Int64GetDatum((int64) copy.writeCalls);
		values[3] = Float8GetDatum(copy.execMs);
		values[4] = Float8GetDatum(copy.userMs);
		values[5] = Float8GetDatum(copy.sysMs);
		values[6] = Int64GetDatum((int64) copy.sharedBlksHit);
		values[7] = Int64GetDatum((int64) copy.sharedBlksRead);
		values[8] = Int64GetDatum((int64) copy.sharedBlksDirtied);
		values[9] = Int64GetDatum((int64) copy.sharedBlksWritten);
		values[10] = Int64GetDatum((int64) copy.tempBlksWritten);
		values[11] = Int64GetDatum((int64) copy.walRecords);
		values[12] = Int64GetDatum((int64) copy.walBytes);
		values[13] = Int64GetDatum((int64) copy.rows);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_RESOURCE));

	PG_RETURN_VOID();
}
