/*-------------------------------------------------------------------------
 *
 * contention.c
 *	  Background worker that samples lock waits on this node.
 *
 * Every citus_observer.contention_interval_ms the worker reads the lock
 * manager's state and, for every backend waiting on a lock, counts one
 * sample against (relation, lock type, mode, row). For row-level waits the
 * contended row is resolved to its primary key value:
 *
 *	- a backend waiting on a tuple lock names the row directly;
 *	- the first backend in line for a row holds the tuple lock and waits on
 *	  the transaction that updated the row, so the row comes from the tuple
 *	  lock it holds.
 *
 * wait time is estimated as samples * interval.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/wait_event.h"

#include "distributed/metadata_cache.h"
#include "distributed/observer/observer.h"
#include "distributed/observer/observer_tables.h"
#include "distributed/tuplestore.h"

typedef struct LockWait
{
	Oid relationId;
	LockTagType lockType;
	LOCKMODE mode;
	bool hasRow;
	BlockNumber block;
	OffsetNumber offset;
} LockWait;

static void SampleLockWaits(void);
static bool HeldTupleLock(LockData *lockData, int pid, Oid databaseId, LockWait *wait);
static void RowKeyText(LockWait *wait, char *buffer, int size);
static void RecordWait(LockWait *wait);

PG_FUNCTION_INFO_V1(citus_observer_contention);


/*
 * RegisterObserverContentionWorker registers the sampler. It must run while
 * shared_preload_libraries are loaded.
 */
void
RegisterObserverContentionWorker(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		return;
	}

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;
	strlcpy(worker.bgw_library_name, "citus", sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_function_name, "CitusObserverContentionMain",
			sizeof(worker.bgw_function_name));
	strlcpy(worker.bgw_name, "citus_observer contention sampler",
			sizeof(worker.bgw_name));
	strlcpy(worker.bgw_type, "citus_observer contention sampler",
			sizeof(worker.bgw_type));
	RegisterBackgroundWorker(&worker);
}


/*
 * CitusObserverContentionMain is the sampler's main loop.
 */
void
CitusObserverContentionMain(Datum arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(ObserverContentionDatabase, NULL, 0);
	pgstat_report_appname("citus_observer contention sampler");

	for (;;)
	{
		int interval = ObserverContentionIntervalMs;

		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 interval, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (!ObserverAreaEnabled(OBS_AREA_CONTENTION))
		{
			continue;
		}

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		if (CitusHasBeenLoaded())
		{
			SampleLockWaits();
		}
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_stat(false);
	}
}


/*
 * SampleLockWaits counts one sample for every backend currently waiting on
 * a lock in this database.
 */
static void
SampleLockWaits(void)
{
	LockData *lockData = GetLockStatusData();

	for (int i = 0; i < lockData->nelements; i++)
	{
		LockInstanceData *instance = &lockData->locks[i];
		LOCKTAG *tag = &instance->locktag;
		LockWait wait;

		if (instance->waitLockMode == NoLock || instance->pid == MyProcPid)
		{
			continue;
		}

		memset(&wait, 0, sizeof(wait));
		wait.lockType = (LockTagType) tag->locktag_type;
		wait.mode = instance->waitLockMode;

		switch (tag->locktag_type)
		{
			case LOCKTAG_RELATION:
			case LOCKTAG_RELATION_EXTEND:
			case LOCKTAG_PAGE:
			{
				if (tag->locktag_field1 != MyDatabaseId)
				{
					continue;
				}
				wait.relationId = tag->locktag_field2;
				break;
			}

			case LOCKTAG_TUPLE:
			{
				if (tag->locktag_field1 != MyDatabaseId)
				{
					continue;
				}
				wait.relationId = tag->locktag_field2;
				wait.hasRow = true;
				wait.block = tag->locktag_field3;
				wait.offset = tag->locktag_field4;
				break;
			}

			case LOCKTAG_TRANSACTION:
			{
				/* rows waits show up as waiting on the updater's transaction */
				HeldTupleLock(lockData, instance->pid, MyDatabaseId, &wait);
				break;
			}

			default:
			{
				break;
			}
		}

		RecordWait(&wait);
	}
}


/*
 * HeldTupleLock finds a tuple lock granted to pid and copies its row into
 * wait. Returns false if pid holds none.
 */
static bool
HeldTupleLock(LockData *lockData, int pid, Oid databaseId, LockWait *wait)
{
	for (int i = 0; i < lockData->nelements; i++)
	{
		LockInstanceData *instance = &lockData->locks[i];
		LOCKTAG *tag = &instance->locktag;

		if (instance->pid != pid || instance->holdMask == 0 ||
			tag->locktag_type != LOCKTAG_TUPLE || tag->locktag_field1 != databaseId)
		{
			continue;
		}

		wait->relationId = tag->locktag_field2;
		wait->hasRow = true;
		wait->block = tag->locktag_field3;
		wait->offset = tag->locktag_field4;
		return true;
	}
	return false;
}


/*
 * RowKeyText formats the primary key of the row a wait is on as
 * "col=value,...", or leaves buffer empty if it cannot be resolved without
 * blocking.
 */
static void
RowKeyText(LockWait *wait, char *buffer, int size)
{
	buffer[0] = '\0';

	if (!wait->hasRow || !OidIsValid(wait->relationId))
	{
		return;
	}

	/* never queue behind a lock: an exclusive lock on the table is possible */
	if (!ConditionalLockRelationOid(wait->relationId, AccessShareLock))
	{
		return;
	}

	Relation relation = try_table_open(wait->relationId, NoLock);
	if (relation == NULL)
	{
		UnlockRelationOid(wait->relationId, AccessShareLock);
		return;
	}

	Oid primaryKeyIndex = RelationGetPrimaryKeyIndex(relation, false);
	if (!OidIsValid(primaryKeyIndex))
	{
		table_close(relation, AccessShareLock);
		return;
	}

	HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(primaryKeyIndex));
	if (!HeapTupleIsValid(indexTuple))
	{
		table_close(relation, AccessShareLock);
		return;
	}
	Form_pg_index index = (Form_pg_index) GETSTRUCT(indexTuple);

	ItemPointerData tid;
	ItemPointerSet(&tid, wait->block, wait->offset);
	TupleTableSlot *slot = table_slot_create(relation, NULL);

	if (table_tuple_fetch_row_version(relation, &tid, SnapshotAny, slot))
	{
		int length = 0;

		for (int column = 0; column < index->indnkeyatts && length < size - 1; column++)
		{
			AttrNumber attnum = index->indkey.values[column];
			bool isNull = false;
			Datum value = slot_getattr(slot, attnum, &isNull);
			Form_pg_attribute attribute = TupleDescAttr(RelationGetDescr(relation),
														attnum - 1);
			char *valueText = "NULL";

			if (!isNull)
			{
				Oid outputFunction = InvalidOid;
				bool isVarlena = false;

				getTypeOutputInfo(attribute->atttypid, &outputFunction, &isVarlena);
				valueText = OidOutputFunctionCall(outputFunction, value);
			}

			length += snprintf(buffer + length, size - length, "%s%s=%s",
							   column > 0 ? "," : "", NameStr(attribute->attname),
							   valueText);
		}
		buffer[size - 1] = '\0';
	}

	ExecDropSingleTupleTableSlot(slot);
	ReleaseSysCache(indexTuple);
	table_close(relation, AccessShareLock);
}


/*
 * RecordWait counts one sample of a wait.
 */
static void
RecordWait(LockWait *wait)
{
	ObsContentionKey key;

	memset(&key, 0, sizeof(key));
	key.relationId = wait->relationId;
	key.lockType = (int16) wait->lockType;
	key.lockMode = (int16) wait->mode;
	RowKeyText(wait, key.rowKey, OBS_ROW_KEY_LEN);

	uint64 shardId = ObserverShardIdForRelation(wait->relationId);

	ObsContentionEntry *entry = ObsTableLockEntry(OBS_TABLE_CONTENTION, &key, NULL);
	if (entry == NULL)
	{
		return;
	}
	entry->shardId = shardId;
	entry->samples++;
	ObsTableUnlockEntry(OBS_TABLE_CONTENTION, entry);
}


/*
 * citus_observer_contention returns the sampled lock waits.
 */
Datum
citus_observer_contention(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsContentionEntry *entry = NULL;

	LWLockAcquire(ObsTableLock(OBS_TABLE_CONTENTION), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_CONTENTION));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsContentionEntry copy;
		Datum values[7];
		bool isNulls[7] = { false };

		SpinLockAcquire(&entry->mutex);
		copy = *entry;
		SpinLockRelease(&entry->mutex);

		if (copy.shardId != 0)
		{
			values[0] = Int64GetDatum((int64) copy.shardId);
		}
		else
		{
			isNulls[0] = true;
		}
		if (OidIsValid(copy.key.relationId))
		{
			values[1] = ObjectIdGetDatum(copy.key.relationId);
		}
		else
		{
			isNulls[1] = true;
		}
		values[2] = CStringGetTextDatum(GetLockNameFromTagType(copy.key.lockType));
		values[3] = CStringGetTextDatum(GetLockmodeName(DEFAULT_LOCKMETHOD,
														copy.key.lockMode));
		if (copy.key.rowKey[0] != '\0')
		{
			values[4] = CStringGetTextDatum(copy.key.rowKey);
		}
		else
		{
			isNulls[4] = true;
		}
		values[5] = Int64GetDatum((int64) copy.samples);
		values[6] = Float8GetDatum((double) copy.samples * ObserverContentionIntervalMs);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_CONTENTION));

	PG_RETURN_VOID();
}
