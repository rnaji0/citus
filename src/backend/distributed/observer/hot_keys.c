/*-------------------------------------------------------------------------
 *
 * hot_keys.c
 *	  Finds the most frequently accessed rows of local shards.
 *
 * When a statement on a shard looks rows up through its primary key with
 * every key column bound to a constant or parameter, the key values are
 * formatted as "col=value,..." and counted in a count-min sketch shared by
 * all backends (see obs_sketch.c). Keys whose estimate is among the
 * largest are kept by value in the sketch's top-K table. Statements that
 * modify or lock rows count as writes, everything else as reads.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "access/stratnum.h"
#include "common/pg_prng.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "distributed/metadata_cache.h"
#include "distributed/observer/obs_sketch.h"
#include "distributed/observer/observer.h"
#include "distributed/tuplestore.h"

#define SKETCH_DEPTH 4
#define SKETCH_WIDTH 16384
#define SKETCH_TOP_K 512

typedef struct KeyWalkerContext
{
	QueryDesc *queryDesc;
	uint8 access;
} KeyWalkerContext;

static ObsSketch *SharedSketch = NULL;

static bool KeyAccessWalker(PlanState *planState, void *context);
static void RecordIndexLookup(KeyWalkerContext *context, Relation heapRelation,
							  Relation indexRelation, List *indexQuals);
static bool BoundValue(Node *node, ParamListInfo params, Datum *value, Oid *type,
					   bool *isNull);

PG_FUNCTION_INFO_V1(citus_observer_hot_keys);


/*
 * ObserverKeySketchShmemSize returns the shared memory the sketch needs.
 */
Size
ObserverKeySketchShmemSize(void)
{
	return obs_sketch_size(SKETCH_DEPTH, SKETCH_WIDTH, SKETCH_TOP_K);
}


/*
 * ObserverKeySketchShmemInit sets up the sketch in freshly created shared
 * memory, or attaches to it.
 */
void
ObserverKeySketchShmemInit(void *memory, bool found)
{
	SharedSketch = (ObsSketch *) memory;
	if (!found)
	{
		obs_sketch_init(SharedSketch, SKETCH_DEPTH, SKETCH_WIDTH, SKETCH_TOP_K);
	}
}


/*
 * ObserverKeySketchUsage reports how many top-K slots are used.
 */
void
ObserverKeySketchUsage(int64 *used, int64 *capacity)
{
	*used = SharedSketch != NULL ? SharedSketch->used : 0;
	*capacity = SharedSketch != NULL ? SharedSketch->k : 0;
}


/*
 * ObserverResetKeySketch clears the sketch.
 */
void
ObserverResetKeySketch(void)
{
	if (SharedSketch == NULL)
	{
		return;
	}
	LWLockAcquire(ObserverKeySketchLock(), LW_EXCLUSIVE);
	obs_sketch_reset(SharedSketch);
	LWLockRelease(ObserverKeySketchLock());
}


/*
 * ObserverRecordKeyAccesses counts the primary key lookups of a statement
 * that was just started.
 */
void
ObserverRecordKeyAccesses(QueryDesc *queryDesc)
{
	if (SharedSketch == NULL || queryDesc->planstate == NULL)
	{
		return;
	}

	if (ObserverKeySampleRate < 1.0 &&
		pg_prng_double(&pg_global_prng_state) >= ObserverKeySampleRate)
	{
		return;
	}

	PlannedStmt *plannedStmt = queryDesc->plannedstmt;
	KeyWalkerContext context = {
		.queryDesc = queryDesc,
		.access = (plannedStmt->commandType != CMD_SELECT ||
				   plannedStmt->rowMarks != NIL) ? OBS_ACCESS_WRITE : OBS_ACCESS_READ
	};

	KeyAccessWalker(queryDesc->planstate, &context);
}


/*
 * KeyAccessWalker visits every plan node and records index scans.
 */
static bool
KeyAccessWalker(PlanState *planState, void *context)
{
	if (planState == NULL)
	{
		return false;
	}

	if (IsA(planState, IndexScanState))
	{
		IndexScanState *scanState = (IndexScanState *) planState;
		IndexScan *scan = (IndexScan *) planState->plan;

		RecordIndexLookup(context, scanState->ss.ss_currentRelation,
						  scanState->iss_RelationDesc, scan->indexqual);
	}
	else if (IsA(planState, IndexOnlyScanState))
	{
		IndexOnlyScanState *scanState = (IndexOnlyScanState *) planState;
		IndexOnlyScan *scan = (IndexOnlyScan *) planState->plan;

		RecordIndexLookup(context, scanState->ss.ss_currentRelation,
						  scanState->ioss_RelationDesc, scan->indexqual);
	}

	return planstate_tree_walker(planState, KeyAccessWalker, context);
}


/*
 * BoundValue returns the value of a constant or an external parameter.
 */
static bool
BoundValue(Node *node, ParamListInfo params, Datum *value, Oid *type, bool *isNull)
{
	if (IsA(node, RelabelType))
	{
		node = (Node *) ((RelabelType *) node)->arg;
	}

	if (IsA(node, Const))
	{
		Const *constant = (Const *) node;

		*value = constant->constvalue;
		*type = constant->consttype;
		*isNull = constant->constisnull;
		return true;
	}

	if (IsA(node, Param) && ((Param *) node)->paramkind == PARAM_EXTERN &&
		params != NULL)
	{
		Param *param = (Param *) node;
		ParamExternData workspace;
		ParamExternData *paramData = NULL;

		if (param->paramid <= 0 || param->paramid > params->numParams)
		{
			return false;
		}

		if (params->paramFetch != NULL)
		{
			paramData = params->paramFetch(params, param->paramid, false, &workspace);
		}
		else
		{
			paramData = &params->params[param->paramid - 1];
		}

		if (paramData == NULL || !OidIsValid(paramData->ptype))
		{
			return false;
		}

		*value = paramData->value;
		*type = paramData->ptype;
		*isNull = paramData->isnull;
		return true;
	}

	return false;
}


/*
 * RecordIndexLookup counts a primary key lookup whose key columns are all
 * bound by equality.
 */
static void
RecordIndexLookup(KeyWalkerContext *context, Relation heapRelation,
				  Relation indexRelation, List *indexQuals)
{
	if (heapRelation == NULL || indexRelation == NULL || indexQuals == NIL ||
		!indexRelation->rd_index->indisprimary)
	{
		return;
	}

	uint64 shardId = ObserverShardIdForRelation(RelationGetRelid(heapRelation));
	if (shardId == INVALID_SHARD_ID)
	{
		return;
	}

	int keyCount = indexRelation->rd_index->indnkeyatts;
	if (keyCount <= 0 || keyCount > INDEX_MAX_KEYS)
	{
		return;
	}

	Datum values[INDEX_MAX_KEYS];
	Oid types[INDEX_MAX_KEYS];
	bool bound[INDEX_MAX_KEYS] = { false };
	int boundCount = 0;
	ListCell *qualCell = NULL;

	foreach(qualCell, indexQuals)
	{
		OpExpr *opExpr = (OpExpr *) lfirst(qualCell);

		if (!IsA(opExpr, OpExpr) || list_length(opExpr->args) != 2)
		{
			continue;
		}

		Node *left = linitial(opExpr->args);
		Node *right = lsecond(opExpr->args);
		if (IsA(left, RelabelType))
		{
			left = (Node *) ((RelabelType *) left)->arg;
		}
		if (!IsA(left, Var))
		{
			continue;
		}

		/* index quals reference index columns by position */
		int column = ((Var *) left)->varattno;
		if (column < 1 || column > keyCount || bound[column - 1])
		{
			continue;
		}

		if (get_op_opfamily_strategy(opExpr->opno,
									 indexRelation->rd_opfamily[column - 1]) !=
			BTEqualStrategyNumber)
		{
			continue;
		}

		bool isNull = false;
		if (!BoundValue(right, context->queryDesc->params, &values[column - 1],
						&types[column - 1], &isNull) || isNull)
		{
			continue;
		}

		bound[column - 1] = true;
		boundCount++;
	}

	if (boundCount != keyCount)
	{
		return;
	}

	char keyText[OBS_SKETCH_KEY_LEN];
	int length = 0;

	keyText[0] = '\0';
	for (int column = 0; column < keyCount && length < OBS_SKETCH_KEY_LEN - 1; column++)
	{
		AttrNumber heapAttribute = indexRelation->rd_index->indkey.values[column];
		const char *columnName =
			NameStr(TupleDescAttr(RelationGetDescr(heapRelation), heapAttribute - 1)->
					attname);
		Oid outputFunction = InvalidOid;
		bool isVarlena = false;

		getTypeOutputInfo(types[column], &outputFunction, &isVarlena);
		char *valueText = OidOutputFunctionCall(outputFunction, values[column]);

		length += snprintf(keyText + length, OBS_SKETCH_KEY_LEN - length, "%s%s=%s",
						   column > 0 ? "," : "", columnName, valueText);
		pfree(valueText);
	}
	if (length >= OBS_SKETCH_KEY_LEN)
	{
		length = OBS_SKETCH_KEY_LEN - 1;
	}

	uint64 hash = obs_hash64(keyText, length, shardId * 2 + context->access);
	uint64 estimate = obs_sketch_increment(SharedSketch, hash);

	if (obs_sketch_wants(SharedSketch, estimate))
	{
		LWLockAcquire(ObserverKeySketchLock(), LW_EXCLUSIVE);
		obs_sketch_offer(SharedSketch, hash, estimate, shardId, context->access, keyText);
		LWLockRelease(ObserverKeySketchLock());
	}
}


/*
 * citus_observer_hot_keys returns the top-K keys, and clears the sketch
 * afterwards when asked to.
 */
Datum
citus_observer_hot_keys(PG_FUNCTION_ARGS)
{
	bool reset = PG_GETARG_BOOL(0);
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	if (SharedSketch == NULL)
	{
		PG_RETURN_VOID();
	}

	ObsTopKEntry *entries = palloc(sizeof(ObsTopKEntry) * SharedSketch->k);

	LWLockAcquire(ObserverKeySketchLock(), reset ? LW_EXCLUSIVE : LW_SHARED);
	int count = obs_sketch_topk(SharedSketch, entries, SharedSketch->k);
	uint64 total = obs_sketch_total(SharedSketch);
	if (reset)
	{
		obs_sketch_reset(SharedSketch);
	}
	LWLockRelease(ObserverKeySketchLock());

	if (reset)
	{
		ObserverSetAreaResetTime(OBS_AREA_KEY);
	}

	for (int i = 0; i < count; i++)
	{
		Datum values[6];
		bool isNulls[6] = { false };

		values[0] = Int64GetDatum((int64) entries[i].shardid);
		values[1] = CStringGetTextDatum(entries[i].access == OBS_ACCESS_WRITE ?
										"write" : "read");
		values[2] = CStringGetTextDatum(entries[i].key);
		values[3] = Int64GetDatum((int64) entries[i].count);
		values[4] = Int64GetDatum((int64) entries[i].error);
		values[5] = Int64GetDatum((int64) total);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}
