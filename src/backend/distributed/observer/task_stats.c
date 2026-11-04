/*-------------------------------------------------------------------------
 *
 * task_stats.c
 *	  Calls, time, rows and bytes of the tasks this node sends to shard
 *	  placements while acting as the coordinator of a distributed query,
 *	  including tasks it executes locally.
 *
 * The adaptive executor reports each finished placement execution here;
 * the local executor reports each locally executed task. Statistics are
 * keyed by (shard, node, command).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "utils/builtins.h"

#include "distributed/metadata_cache.h"
#include "distributed/observer/observer.h"
#include "distributed/observer/observer_tables.h"
#include "distributed/tuplestore.h"

static const char *const ObsCommandNames[OBS_COMMAND_COUNT] = {
	[OBS_COMMAND_SELECT] = "select",
	[OBS_COMMAND_INSERT] = "insert",
	[OBS_COMMAND_UPDATE] = "update",
	[OBS_COMMAND_DELETE] = "delete",
	[OBS_COMMAND_MERGE] = "merge",
	[OBS_COMMAND_DDL] = "ddl",
	[OBS_COMMAND_REPARTITION] = "repartition",
	[OBS_COMMAND_OTHER] = "other",
};

/* command of the distributed statement whose tasks are running */
static ObsCommand CurrentCommand = OBS_COMMAND_OTHER;

/*
 * Whether that statement targets a single shard. Statements that fan out to
 * many shards say nothing about which shards belong together, so their
 * tasks are left out of transaction co-access.
 */
static bool CurrentStatementTargeted = true;

/* distribution column value the statement was routed by, if any */
static Const *CurrentPartitionKeyValue = NULL;

static ObsCommand CommandForCmdType(CmdType commandType);
static ObsCommand CommandForTask(Task *task);

PG_FUNCTION_INFO_V1(citus_observer_task_stats);


/*
 * CommandForCmdType maps a statement type to the command recorded for its
 * modify tasks.
 */
static ObsCommand
CommandForCmdType(CmdType commandType)
{
	switch (commandType)
	{
		case CMD_SELECT:
		{
			return OBS_COMMAND_SELECT;
		}

		case CMD_INSERT:
		{
			return OBS_COMMAND_INSERT;
		}

		case CMD_UPDATE:
		{
			return OBS_COMMAND_UPDATE;
		}

		case CMD_DELETE:
		{
			return OBS_COMMAND_DELETE;
		}

		case CMD_MERGE:
		{
			return OBS_COMMAND_MERGE;
		}

		default:
		{
			return OBS_COMMAND_OTHER;
		}
	}
}


/*
 * CommandForTask classifies a task by what it does on the placement.
 */
static ObsCommand
CommandForTask(Task *task)
{
	switch (task->taskType)
	{
		case READ_TASK:
		{
			return OBS_COMMAND_SELECT;
		}

		case MODIFY_TASK:
		{
			return CurrentCommand == OBS_COMMAND_SELECT ? OBS_COMMAND_OTHER :
				   CurrentCommand;
		}

		case DDL_TASK:
		case VACUUM_ANALYZE_TASK:
		{
			return OBS_COMMAND_DDL;
		}

		case MAP_TASK:
		case MERGE_TASK:
		case MAP_OUTPUT_FETCH_TASK:
		case MERGE_FETCH_TASK:
		{
			return OBS_COMMAND_REPARTITION;
		}

		default:
		{
			return OBS_COMMAND_OTHER;
		}
	}
}


/*
 * ObserverBeginDistributedExecution remembers the statement type and fan-out
 * of the distributed execution that is about to run its tasks.
 */
void
ObserverBeginDistributedExecution(CmdType commandType, int taskCount,
								  Const *partitionKeyValue)
{
	CurrentCommand = CommandForCmdType(commandType);
	CurrentStatementTargeted = taskCount <= 1;
	CurrentPartitionKeyValue = partitionKeyValue;
}


/*
 * ObserverRecordTaskExecution records one finished (or failed) execution of
 * a task on a placement and adds the task to the current distributed
 * transaction's trace.
 */
void
ObserverRecordTaskExecution(Task *task, int32 nodeId, bool local, bool succeeded,
							uint64 durationUs, uint64 rows, uint64 bytes)
{
	if (task == NULL || task->anchorShardId == INVALID_SHARD_ID)
	{
		return;
	}

	if (succeeded)
	{
		ObserverTraceTask(task, nodeId, CurrentStatementTargeted, CurrentPartitionKeyValue);
	}

	if (!ObserverAreaEnabled(OBS_AREA_TASK))
	{
		return;
	}

	Oid relationId = InvalidOid;
	uint32 colocationId = 0;
	int shardIndex = 0;
	bool isHashDistributed = false;

	if (!LookupShardPosition(task->anchorShardId, &relationId, &colocationId,
							 &shardIndex, &isHashDistributed))
	{
		return;
	}

	ObsTaskKey key;

	memset(&key, 0, sizeof(key));
	key.shardId = task->anchorShardId;
	key.nodeId = nodeId;
	key.command = CommandForTask(task);

	ObsTaskEntry *entry = ObsTableLockEntry(OBS_TABLE_TASK, &key, NULL);
	if (entry == NULL)
	{
		return;
	}

	entry->colocationId = (int32) colocationId;
	entry->calls++;
	if (!succeeded)
	{
		entry->failures++;
	}
	if (local)
	{
		entry->localCalls++;
	}
	entry->rows += rows;
	entry->bytes += bytes;
	entry->totalUs += (double) durationUs;
	if ((double) durationUs > entry->maxUs)
	{
		entry->maxUs = (double) durationUs;
	}

	ObsTableUnlockEntry(OBS_TABLE_TASK, entry);
}


/*
 * ObserverEndDistributedExecution is called once a distributed execution
 * finished all its tasks, to count it against its query fingerprint.
 */
void
ObserverEndDistributedExecution(DistributedPlan *distributedPlan, List *taskList,
								uint64 queryId, double elapsedMs)
{
	ObserverCountPlanExecution(distributedPlan, taskList, queryId, elapsedMs);
	CurrentCommand = OBS_COMMAND_OTHER;
	CurrentStatementTargeted = true;
	CurrentPartitionKeyValue = NULL;
}


/*
 * citus_observer_task_stats returns the task statistics table.
 */
Datum
citus_observer_task_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsTaskEntry *entry = NULL;

	LWLockAcquire(ObsTableLock(OBS_TABLE_TASK), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_TASK));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsTaskEntry copy;
		Datum values[11];
		bool isNulls[11] = { false };

		SpinLockAcquire(&entry->mutex);
		copy = *entry;
		SpinLockRelease(&entry->mutex);

		values[0] = Int64GetDatum((int64) copy.key.shardId);
		values[1] = Int32GetDatum(copy.key.nodeId);
		values[2] = Int32GetDatum(copy.colocationId);
		values[3] = CStringGetTextDatum(ObsCommandNames[copy.key.command]);
		values[4] = Int64GetDatum((int64) copy.calls);
		values[5] = Int64GetDatum((int64) copy.failures);
		values[6] = Int64GetDatum((int64) copy.localCalls);
		values[7] = Float8GetDatum(copy.totalUs / 1000.0);
		values[8] = Float8GetDatum(copy.maxUs / 1000.0);
		values[9] = Int64GetDatum((int64) copy.rows);
		values[10] = Int64GetDatum((int64) copy.bytes);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_TASK));

	PG_RETURN_VOID();
}
