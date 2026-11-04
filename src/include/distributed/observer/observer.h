/*-------------------------------------------------------------------------
 *
 * observer.h
 *	  Engine-level telemetry collected inside Citus: per-task and per-shard
 *	  execution statistics, planner decisions, hot keys, distributed
 *	  transaction traces, lock contention and shard move/split timings.
 *
 * Statistics live in fixed-size shared hash tables, one per area, and
 * discrete events in a sequenced ring buffer. Everything is exposed through
 * the functions of the citus_observer extension.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CITUS_OBSERVER_H
#define CITUS_OBSERVER_H

#include "postgres.h"

#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "distributed/errormessage.h"
#include "distributed/multi_physical_planner.h"

/* areas of instrumentation, each with its own shared table and switch */
typedef enum ObsArea
{
	OBS_AREA_TASK = 0,
	OBS_AREA_RESOURCE,
	OBS_AREA_PLAN,
	OBS_AREA_KEY,
	OBS_AREA_TXN,
	OBS_AREA_CONTENTION,
	OBS_AREA_ACTION,
	OBS_AREA_COUNT
} ObsArea;

/* shared hash tables */
typedef enum ObsTableId
{
	OBS_TABLE_TASK = 0,
	OBS_TABLE_RESOURCE,
	OBS_TABLE_PLAN,
	OBS_TABLE_COLUMN,
	OBS_TABLE_JOIN,
	OBS_TABLE_GROUP_COACCESS,
	OBS_TABLE_TENANT_COACCESS,
	OBS_TABLE_CONTENTION,
	OBS_TABLE_COUNT
} ObsTableId;

/* command classification used for task statistics */
typedef enum ObsCommand
{
	OBS_COMMAND_SELECT = 0,
	OBS_COMMAND_INSERT,
	OBS_COMMAND_UPDATE,
	OBS_COMMAND_DELETE,
	OBS_COMMAND_MERGE,
	OBS_COMMAND_DDL,
	OBS_COMMAND_REPARTITION,
	OBS_COMMAND_OTHER,
	OBS_COMMAND_COUNT
} ObsCommand;

/* phases of a coordinated commit */
typedef enum ObsCommitPhase
{
	OBS_COMMIT_PHASE_PREPARE = 0,
	OBS_COMMIT_PHASE_COMMIT
} ObsCommitPhase;

/* settings */
extern bool ObserverEnabled;
extern bool ObserverTrackTasks;
extern bool ObserverTrackResources;
extern bool ObserverTrackPlans;
extern bool ObserverTrackKeys;
extern bool ObserverTrackTransactions;
extern bool ObserverTrackContention;
extern bool ObserverTrackActions;
extern double ObserverKeySampleRate;
extern double ObserverTxnEventSampleRate;
extern int ObserverMaxEntries;
extern int ObserverEventSlots;
extern int ObserverContentionIntervalMs;
extern char *ObserverContentionDatabase;

/* observer.c: setup, shared tables and events */
extern void InitializeObserver(void);
extern bool ObserverAreaEnabled(ObsArea area);
extern void *ObsTableLockEntry(ObsTableId table, const void *key, bool *created);
extern void ObsTableUnlockEntry(ObsTableId table, void *entry);
extern HTAB * ObsTableHash(ObsTableId table);
extern LWLock * ObsTableLock(ObsTableId table);
extern void ObsTableReset(ObsTableId table);
extern void ObserverPushEvent(const char *kind, const char *payload, int length);
extern TimestampTz ObserverAreaResetTime(ObsArea area);
extern void ObserverSetAreaResetTime(ObsArea area);
extern void ObsEscapeJsonString(StringInfo buf, const char *str);

/* shard_lookup.c: is a local relation a shard, and of what */
extern uint64 ObserverShardIdForRelation(Oid relationId);
extern void InitializeObserverShardLookup(void);

/* task_stats.c: tasks sent to placements by this coordinator */
extern void ObserverBeginDistributedExecution(CmdType commandType, int taskCount,
											  Const *partitionKeyValue);
extern void ObserverRecordTaskExecution(Task *task, int32 nodeId, bool local,
										bool succeeded, uint64 durationUs,
										uint64 rows, uint64 bytes);
extern void ObserverEndDistributedExecution(DistributedPlan *distributedPlan,
											List *taskList, uint64 queryId,
											double elapsedMs);

/* resource_stats.c: executor and utility hooks */
extern void InitializeObserverExecutorHooks(void);

/* hot_keys.c */
extern Size ObserverKeySketchShmemSize(void);
extern void ObserverKeySketchShmemInit(void *memory, bool found);
extern LWLock * ObserverKeySketchLock(void);
extern void ObserverKeySketchUsage(int64 *used, int64 *capacity);
extern void ObserverRecordKeyAccesses(QueryDesc *queryDesc);
extern void ObserverResetKeySketch(void);

/* plan_stats.c */
extern void ObserverAttachRouterFailure(DistributedPlan *distributedPlan,
										const char *reason);
extern void ObserverAnalyzeQueryColumns(Query *originalQuery);
extern void ObserverRecordPlan(Query *originalQuery, DistributedPlan *distributedPlan);
extern void ObserverCountPlanExecution(DistributedPlan *distributedPlan, List *taskList,
									   uint64 queryId, double elapsedMs);

/* txn_trace.c */
extern void ObserverTraceTask(Task *task, int32 nodeId, bool targeted,
							  Const *statementPartitionKeyValue);
extern void ObserverCommitPhase(ObsCommitPhase phase, uint64 durationUs, bool twoPhase);
extern void ObserverDistributedTransactionEnd(bool committed);
extern void ObserverTxnSummaryShmemInit(void *memory, bool found);
extern Size ObserverTxnSummaryShmemSize(void);
extern void ObserverResetTxnSummary(void);

/* contention.c */
extern void RegisterObserverContentionWorker(void);
PGDLLEXPORT void CitusObserverContentionMain(Datum arg);

/* action_trace.c */
extern void ObserverShardTransferBegin(const char *operation, List *shardIntervalList,
									   const char *sourceName, int sourcePort,
									   const char *targetName, int targetPort);
extern void ObserverShardTransferStatus(List *shardIntervalList, int status);
extern void ObserverSplitBegin(const char *operation, List *shardIntervalList,
							   const char *mode);
extern void ObserverActionEnd(bool succeeded);
extern void ObserverActionAbort(void);

#endif /* CITUS_OBSERVER_H */
