/*-------------------------------------------------------------------------
 *
 * plan_stats.c
 *	  How distributed queries are planned and executed, why they could not
 *	  be planned as single-shard router queries, and how they use columns.
 *
 * At plan time each query fingerprint (queryId, as in pg_stat_statements)
 * gets its plan kind and, when the router planner gave up on it, the
 * router planner's reason. The first time a backend plans a fingerprint
 * its query tree is walked to record which columns of Citus tables it
 * compares to constants, lists, ranges or other columns, and groups by.
 * These are the candidates for distribution columns and colocation.
 *
 * At execution time the adaptive executor reports the final task list, so
 * calls of prepared statements with deferred shard pruning are counted
 * with the number of shards they really touched.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "distributed/colocation_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/observer/observer.h"
#include "distributed/observer/observer_tables.h"
#include "distributed/tuplestore.h"

#define ROUTER_FAILURE_SLOTS 8
#define MULTI_SHARD_MODIFY_REASON "modification touches more than one shard"

static const char *const ObsPlanKindNames[OBS_PLAN_KIND_COUNT] = {
	[OBS_PLAN_FAST_PATH_ROUTER] = "fast_path_router",
	[OBS_PLAN_ROUTER] = "router",
	[OBS_PLAN_MULTI_SHARD] = "multi_shard",
	[OBS_PLAN_REPARTITION] = "repartition",
	[OBS_PLAN_INSERT_SELECT_PUSHDOWN] = "insert_select_pushdown",
	[OBS_PLAN_INSERT_SELECT_REPARTITION] = "insert_select_repartition",
	[OBS_PLAN_INSERT_SELECT_COORDINATOR] = "insert_select_coordinator",
	[OBS_PLAN_MERGE] = "merge",
	[OBS_PLAN_RECURSIVE] = "recursive",
};

static const char *const ObsUsageNames[OBS_USAGE_COUNT] = {
	[OBS_USAGE_EQ] = "eq",
	[OBS_USAGE_IN] = "in",
	[OBS_USAGE_RANGE] = "range",
	[OBS_USAGE_JOIN] = "join",
	[OBS_USAGE_GROUP] = "group",
};

/* router planner failures waiting to be picked up by ObserverRecordPlan */
typedef struct RouterFailureSlot
{
	DistributedPlan *plan;
	char reason[OBS_REASON_LEN];
} RouterFailureSlot;

static RouterFailureSlot RouterFailures[ROUTER_FAILURE_SLOTS];
static int NextRouterFailureSlot = 0;

/* fingerprints this backend already analyzed */
static HTAB *AnalyzedQueries = NULL;

/* one column use found while walking a query */
typedef struct ColumnUse
{
	Oid relationId;
	AttrNumber attnum;
	ObsColumnUsage usage;
} ColumnUse;

typedef struct JoinUse
{
	Oid leftRelationId;
	AttrNumber leftAttnum;
	Oid rightRelationId;
	AttrNumber rightAttnum;
} JoinUse;

typedef struct AnalysisContext
{
	List *queryStack;           /* innermost Query first */
	List *columnUses;
	List *joinUses;
} AnalysisContext;

static ObsPlanKind PlanKind(DistributedPlan *distributedPlan, List *taskList);
static const char * TakeRouterFailure(DistributedPlan *distributedPlan);
static bool QueryWalker(Node *node, AnalysisContext *context);
static bool ClauseWalker(Node *node, AnalysisContext *context);
static void AnalyzeQueryLevel(Query *query, AnalysisContext *context);
static bool ResolveColumn(AnalysisContext *context, Node *node, Oid *relationId,
						  AttrNumber *attnum);
static bool IsBoundExpression(Node *node);
static void AddColumnUse(AnalysisContext *context, Oid relationId, AttrNumber attnum,
						 ObsColumnUsage usage);
static bool IsDistributionColumn(Oid relationId, AttrNumber attnum);
static void StoreAnalysis(uint64 queryId, AnalysisContext *context);

PG_FUNCTION_INFO_V1(citus_observer_query_plans);
PG_FUNCTION_INFO_V1(citus_observer_column_usage);
PG_FUNCTION_INFO_V1(citus_observer_join_edges);


/*
 * ObserverAttachRouterFailure remembers why the router planner could not
 * plan the query that ended up as distributedPlan.
 */
void
ObserverAttachRouterFailure(DistributedPlan *distributedPlan, const char *reason)
{
	if (distributedPlan == NULL || reason == NULL)
	{
		return;
	}

	for (int i = 0; i < ROUTER_FAILURE_SLOTS; i++)
	{
		if (RouterFailures[i].plan == distributedPlan)
		{
			strlcpy(RouterFailures[i].reason, reason, OBS_REASON_LEN);
			return;
		}
	}

	RouterFailureSlot *slot = &RouterFailures[NextRouterFailureSlot];
	NextRouterFailureSlot = (NextRouterFailureSlot + 1) % ROUTER_FAILURE_SLOTS;
	slot->plan = distributedPlan;
	strlcpy(slot->reason, reason, OBS_REASON_LEN);
}


/*
 * TakeRouterFailure returns and forgets the router failure attached to a
 * plan, or NULL.
 */
static const char *
TakeRouterFailure(DistributedPlan *distributedPlan)
{
	static char reason[OBS_REASON_LEN];

	for (int i = 0; i < ROUTER_FAILURE_SLOTS; i++)
	{
		if (RouterFailures[i].plan == distributedPlan)
		{
			strlcpy(reason, RouterFailures[i].reason, OBS_REASON_LEN);
			RouterFailures[i].plan = NULL;
			return reason;
		}
	}
	return NULL;
}


/*
 * PlanKind classifies a distributed plan. taskList is the plan's task list
 * at the time of the call.
 */
static ObsPlanKind
PlanKind(DistributedPlan *distributedPlan, List *taskList)
{
	Job *job = distributedPlan->workerJob;

	if (distributedPlan->modifyQueryViaCoordinatorOrRepartition != NULL)
	{
		if (distributedPlan->modifyQueryViaCoordinatorOrRepartition->commandType ==
			CMD_MERGE)
		{
			return OBS_PLAN_MERGE;
		}
		return distributedPlan->modifyWithSelectMethod == MODIFY_WITH_SELECT_REPARTITION ?
			   OBS_PLAN_INSERT_SELECT_REPARTITION : OBS_PLAN_INSERT_SELECT_COORDINATOR;
	}

	if (job == NULL)
	{
		return OBS_PLAN_MULTI_SHARD;
	}

	if (job->dependentJobList != NIL)
	{
		return OBS_PLAN_REPARTITION;
	}

	if (job->jobQuery != NULL && job->jobQuery->commandType == CMD_MERGE)
	{
		return OBS_PLAN_MERGE;
	}

	if (job->jobQuery != NULL && job->jobQuery->commandType == CMD_INSERT)
	{
		RangeTblEntry *rangeTableEntry = NULL;
		foreach_declared_ptr(rangeTableEntry, job->jobQuery->rtable)
		{
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				return OBS_PLAN_INSERT_SELECT_PUSHDOWN;
			}
		}
	}

	if (distributedPlan->subPlanList != NIL)
	{
		return OBS_PLAN_RECURSIVE;
	}

	if (distributedPlan->fastPathRouterPlan)
	{
		return OBS_PLAN_FAST_PATH_ROUTER;
	}

	if (job->deferredPruning || list_length(taskList) <= 1)
	{
		return OBS_PLAN_ROUTER;
	}

	return OBS_PLAN_MULTI_SHARD;
}


/*
 * ObserverRecordPlan records the kind of a freshly created distributed
 * plan and, if the router planner rejected the query, its reason.
 */
void
ObserverRecordPlan(Query *originalQuery, DistributedPlan *distributedPlan)
{
	const char *reason = TakeRouterFailure(distributedPlan);

	if (!ObserverAreaEnabled(OBS_AREA_PLAN) || distributedPlan->planningError != NULL ||
		originalQuery == NULL || originalQuery->queryId == UINT64CONST(0))
	{
		return;
	}

	List *taskList = distributedPlan->workerJob != NULL ?
					 distributedPlan->workerJob->taskList : NIL;
	ObsPlanKind kind = PlanKind(distributedPlan, taskList);

	if (reason == NULL && kind == OBS_PLAN_MULTI_SHARD &&
		originalQuery->commandType != CMD_SELECT)
	{
		reason = MULTI_SHARD_MODIFY_REASON;
	}

	ObsPlanKey key;

	memset(&key, 0, sizeof(key));
	key.queryId = originalQuery->queryId;
	key.planKind = kind;

	ObsPlanEntry *entry = ObsTableLockEntry(OBS_TABLE_PLAN, &key, NULL);
	if (entry == NULL)
	{
		return;
	}

	if (reason != NULL && entry->reason[0] == '\0')
	{
		strlcpy(entry->reason, reason, OBS_REASON_LEN);
	}
	if (distributedPlan->workerJob != NULL)
	{
		entry->colocationId = (int32) distributedPlan->workerJob->colocationId;
	}
	entry->subplans = list_length(distributedPlan->subPlanList);

	ObsTableUnlockEntry(OBS_TABLE_PLAN, entry);
}


/*
 * ObserverCountPlanExecution counts one execution of a distributed plan
 * with its final task list.
 */
void
ObserverCountPlanExecution(DistributedPlan *distributedPlan, List *taskList,
						   uint64 queryId, double elapsedMs)
{
	if (!ObserverAreaEnabled(OBS_AREA_PLAN) || distributedPlan == NULL ||
		queryId == UINT64CONST(0))
	{
		return;
	}

	int taskCount = list_length(taskList);
	ObsPlanKey key;

	memset(&key, 0, sizeof(key));
	key.queryId = queryId;
	key.planKind = PlanKind(distributedPlan, taskList);

	ObsPlanEntry *entry = ObsTableLockEntry(OBS_TABLE_PLAN, &key, NULL);
	if (entry == NULL)
	{
		return;
	}

	entry->calls++;
	if (taskCount <= 1)
	{
		entry->singleShardCalls++;
	}
	else
	{
		entry->multiShardCalls++;
	}
	entry->totalTasks += taskCount;
	if (taskCount > entry->maxTasks)
	{
		entry->maxTasks = taskCount;
	}
	entry->totalMs += elapsedMs;
	if (distributedPlan->workerJob != NULL && entry->colocationId == 0)
	{
		entry->colocationId = (int32) distributedPlan->workerJob->colocationId;
	}

	ObsTableUnlockEntry(OBS_TABLE_PLAN, entry);
}


/*
 * ObserverAnalyzeQueryColumns records the column usage of a query
 * fingerprint the first time this backend plans it.
 */
void
ObserverAnalyzeQueryColumns(Query *originalQuery)
{
	bool found = false;

	if (!ObserverAreaEnabled(OBS_AREA_PLAN) || originalQuery == NULL ||
		originalQuery->queryId == UINT64CONST(0))
	{
		return;
	}

	if (AnalyzedQueries == NULL)
	{
		HASHCTL info;

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(uint64);
		info.entrysize = sizeof(uint64);
		info.hcxt = TopMemoryContext;
		AnalyzedQueries = hash_create("citus_observer analyzed queries", 1024, &info,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	uint64 queryId = originalQuery->queryId;
	hash_search(AnalyzedQueries, &queryId, HASH_ENTER, &found);
	if (found)
	{
		return;
	}

	/* the table only grows; start over if a backend plans a huge variety */
	if (hash_get_num_entries(AnalyzedQueries) > ObserverMaxEntries * 4L)
	{
		hash_destroy(AnalyzedQueries);
		AnalyzedQueries = NULL;
	}

	AnalysisContext context = { 0 };

	QueryWalker((Node *) originalQuery, &context);
	StoreAnalysis(queryId, &context);
}


/*
 * QueryWalker analyzes every query level: the top-level query, subqueries,
 * CTEs and sublinks.
 */
static bool
QueryWalker(Node *node, AnalysisContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		context->queryStack = lcons(query, context->queryStack);
		AnalyzeQueryLevel(query, context);
		bool result = query_tree_walker(query, QueryWalker, context, 0);
		context->queryStack = list_delete_first(context->queryStack);
		return result;
	}

	return expression_tree_walker(node, QueryWalker, context);
}


/*
 * AnalyzeQueryLevel records the WHERE/JOIN conditions and GROUP BY columns
 * of one query level.
 */
static void
AnalyzeQueryLevel(Query *query, AnalysisContext *context)
{
	ClauseWalker((Node *) query->jointree, context);

	SortGroupClause *groupClause = NULL;
	foreach_declared_ptr(groupClause, query->groupClause)
	{
		TargetEntry *targetEntry = get_sortgroupclause_tle(groupClause,
														   query->targetList);
		Oid relationId = InvalidOid;
		AttrNumber attnum = InvalidAttrNumber;

		if (ResolveColumn(context, (Node *) targetEntry->expr, &relationId, &attnum))
		{
			AddColumnUse(context, relationId, attnum, OBS_USAGE_GROUP);
		}
	}
}


/*
 * ResolveColumn maps a column reference of the current query level to a
 * Citus table column.
 */
static bool
ResolveColumn(AnalysisContext *context, Node *node, Oid *relationId,
			  AttrNumber *attnum)
{
	while (node != NULL && IsA(node, RelabelType))
	{
		node = (Node *) ((RelabelType *) node)->arg;
	}

	if (node == NULL || !IsA(node, Var))
	{
		return false;
	}

	Var *column = (Var *) node;

	if (column->varlevelsup >= list_length(context->queryStack))
	{
		return false;
	}

	Query *query = list_nth(context->queryStack, column->varlevelsup);

	if (column->varno < 1 || column->varno > list_length(query->rtable) ||
		column->varattno <= 0)
	{
		return false;
	}

	RangeTblEntry *rangeTableEntry = rt_fetch(column->varno, query->rtable);

	/* grouped columns point at the query's GROUP entry, which lists the expressions */
	if (rangeTableEntry->rtekind == RTE_GROUP)
	{
		if (column->varlevelsup != 0 ||
			column->varattno > list_length(rangeTableEntry->groupexprs))
		{
			return false;
		}
		return ResolveColumn(context, list_nth(rangeTableEntry->groupexprs,
											   column->varattno - 1),
							 relationId, attnum);
	}

	if (rangeTableEntry->rtekind != RTE_RELATION || !IsCitusTable(rangeTableEntry->relid))
	{
		return false;
	}

	*relationId = rangeTableEntry->relid;
	*attnum = column->varattno;
	return true;
}


/*
 * IsBoundExpression returns whether an expression is fixed for one
 * execution: a constant, a parameter, or a function of those.
 */
static bool
IsBoundExpression(Node *node)
{
	return node != NULL && !contain_var_clause(node) && !IsA(node, SubLink);
}


/*
 * ClauseWalker classifies comparison operators in a qualification tree.
 */
static bool
ClauseWalker(Node *node, AnalysisContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, SubLink) || IsA(node, Query))
	{
		/* QueryWalker analyzes subqueries at their own level */
		return false;
	}

	if (IsA(node, OpExpr) && list_length(((OpExpr *) node)->args) == 2)
	{
		OpExpr *opExpr = (OpExpr *) node;
		Node *left = linitial(opExpr->args);
		Node *right = lsecond(opExpr->args);
		char *operatorName = get_opname(opExpr->opno);
		Oid leftRelationId = InvalidOid;
		Oid rightRelationId = InvalidOid;
		AttrNumber leftAttnum = InvalidAttrNumber;
		AttrNumber rightAttnum = InvalidAttrNumber;
		bool leftIsColumn = ResolveColumn(context, left, &leftRelationId, &leftAttnum);
		bool rightIsColumn = ResolveColumn(context, right, &rightRelationId,
										   &rightAttnum);
		ObsColumnUsage usage = OBS_USAGE_COUNT;

		if (operatorName == NULL)
		{
			return false;
		}
		if (strcmp(operatorName, "=") == 0)
		{
			usage = OBS_USAGE_EQ;
		}
		else if (strcmp(operatorName, "<") == 0 || strcmp(operatorName, "<=") == 0 ||
				 strcmp(operatorName, ">") == 0 || strcmp(operatorName, ">=") == 0)
		{
			usage = OBS_USAGE_RANGE;
		}

		if (usage == OBS_USAGE_EQ && leftIsColumn && rightIsColumn &&
			(leftRelationId != rightRelationId || leftAttnum != rightAttnum))
		{
			JoinUse *join = palloc0(sizeof(JoinUse));

			/* store each pair in one canonical order */
			if (leftRelationId > rightRelationId ||
				(leftRelationId == rightRelationId && leftAttnum > rightAttnum))
			{
				join->leftRelationId = rightRelationId;
				join->leftAttnum = rightAttnum;
				join->rightRelationId = leftRelationId;
				join->rightAttnum = leftAttnum;
			}
			else
			{
				join->leftRelationId = leftRelationId;
				join->leftAttnum = leftAttnum;
				join->rightRelationId = rightRelationId;
				join->rightAttnum = rightAttnum;
			}
			context->joinUses = lappend(context->joinUses, join);
			AddColumnUse(context, leftRelationId, leftAttnum, OBS_USAGE_JOIN);
			AddColumnUse(context, rightRelationId, rightAttnum, OBS_USAGE_JOIN);
		}
		else if (usage != OBS_USAGE_COUNT && leftIsColumn && IsBoundExpression(right))
		{
			AddColumnUse(context, leftRelationId, leftAttnum, usage);
		}
		else if (usage != OBS_USAGE_COUNT && rightIsColumn && IsBoundExpression(left))
		{
			AddColumnUse(context, rightRelationId, rightAttnum, usage);
		}
		return false;
	}

	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *arrayExpr = (ScalarArrayOpExpr *) node;
		char *operatorName = get_opname(arrayExpr->opno);
		Oid relationId = InvalidOid;
		AttrNumber attnum = InvalidAttrNumber;

		if (arrayExpr->useOr && operatorName != NULL && strcmp(operatorName, "=") == 0 &&
			ResolveColumn(context, linitial(arrayExpr->args), &relationId, &attnum) &&
			IsBoundExpression(lsecond(arrayExpr->args)))
		{
			AddColumnUse(context, relationId, attnum, OBS_USAGE_IN);
		}
		return false;
	}

	return expression_tree_walker(node, ClauseWalker, context);
}


/*
 * AddColumnUse appends one column use found in the query.
 */
static void
AddColumnUse(AnalysisContext *context, Oid relationId, AttrNumber attnum,
			 ObsColumnUsage usage)
{
	ColumnUse *use = palloc0(sizeof(ColumnUse));

	use->relationId = relationId;
	use->attnum = attnum;
	use->usage = usage;
	context->columnUses = lappend(context->columnUses, use);
}


/*
 * IsDistributionColumn returns whether attnum is the distribution column of
 * a distributed table.
 */
static bool
IsDistributionColumn(Oid relationId, AttrNumber attnum)
{
	CitusTableCacheEntry *cacheEntry = LookupCitusTableCacheEntry(relationId);

	return cacheEntry != NULL && cacheEntry->partitionColumn != NULL &&
		   cacheEntry->partitionColumn->varattno == attnum;
}


/*
 * StoreAnalysis writes the uses found for a fingerprint to the shared
 * column and join tables.
 */
static void
StoreAnalysis(uint64 queryId, AnalysisContext *context)
{
	ListCell *useCell = NULL;
	foreach(useCell, context->columnUses)
	{
		ColumnUse *use = lfirst(useCell);
		int occurrences = 0;
		bool seenBefore = false;
		ListCell *otherCell = NULL;

		/* count every use of this (column, usage), once per distinct key */
		foreach(otherCell, context->columnUses)
		{
			ColumnUse *other = lfirst(otherCell);

			if (other->relationId != use->relationId || other->attnum != use->attnum ||
				other->usage != use->usage)
			{
				continue;
			}
			if (otherCell < useCell)
			{
				seenBefore = true;
				break;
			}
			occurrences++;
		}
		if (seenBefore)
		{
			continue;
		}

		ObsColumnKey key;
		bool isDistributionColumn = IsDistributionColumn(use->relationId, use->attnum);

		memset(&key, 0, sizeof(key));
		key.queryId = queryId;
		key.relationId = use->relationId;
		key.attnum = use->attnum;
		key.usage = use->usage;

		ObsColumnEntry *entry = ObsTableLockEntry(OBS_TABLE_COLUMN, &key, NULL);
		if (entry == NULL)
		{
			continue;
		}
		entry->isDistributionColumn = isDistributionColumn;
		entry->occurrences = occurrences;
		ObsTableUnlockEntry(OBS_TABLE_COLUMN, entry);
	}

	JoinUse *join = NULL;
	foreach_declared_ptr(join, context->joinUses)
	{
		ObsJoinKey key;
		bool leftIsReference = IsCitusTableType(join->leftRelationId, REFERENCE_TABLE);
		bool rightIsReference = IsCitusTableType(join->rightRelationId,
												 REFERENCE_TABLE);
		bool colocated = TablesColocated(join->leftRelationId, join->rightRelationId);
		bool pushdown = leftIsReference || rightIsReference ||
						(colocated &&
						 IsDistributionColumn(join->leftRelationId, join->leftAttnum) &&
						 IsDistributionColumn(join->rightRelationId, join->rightAttnum));

		memset(&key, 0, sizeof(key));
		key.queryId = queryId;
		key.leftRelationId = join->leftRelationId;
		key.leftAttnum = join->leftAttnum;
		key.rightRelationId = join->rightRelationId;
		key.rightAttnum = join->rightAttnum;

		ObsJoinEntry *entry = ObsTableLockEntry(OBS_TABLE_JOIN, &key, NULL);
		if (entry == NULL)
		{
			continue;
		}
		entry->colocated = colocated;
		entry->pushdown = pushdown;
		ObsTableUnlockEntry(OBS_TABLE_JOIN, entry);
	}
}


/*
 * citus_observer_query_plans returns the plan table.
 */
Datum
citus_observer_query_plans(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsPlanEntry *entry = NULL;

	LWLockAcquire(ObsTableLock(OBS_TABLE_PLAN), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_PLAN));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsPlanEntry copy;
		Datum values[11];
		bool isNulls[11] = { false };

		SpinLockAcquire(&entry->mutex);
		copy = *entry;
		SpinLockRelease(&entry->mutex);

		values[0] = Int64GetDatum((int64) copy.key.queryId);
		values[1] = CStringGetTextDatum(ObsPlanKindNames[copy.key.planKind]);
		if (copy.reason[0] != '\0')
		{
			values[2] = CStringGetTextDatum(copy.reason);
		}
		else
		{
			isNulls[2] = true;
		}
		values[3] = Int32GetDatum(copy.colocationId);
		values[4] = Int64GetDatum((int64) copy.calls);
		values[5] = Int64GetDatum((int64) copy.singleShardCalls);
		values[6] = Int64GetDatum((int64) copy.multiShardCalls);
		values[7] = Int64GetDatum((int64) copy.totalTasks);
		values[8] = Int32GetDatum(copy.maxTasks);
		values[9] = Int32GetDatum(copy.subplans);
		values[10] = Float8GetDatum(copy.totalMs);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_PLAN));

	PG_RETURN_VOID();
}


/*
 * citus_observer_column_usage returns the column usage table.
 */
Datum
citus_observer_column_usage(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsColumnEntry *entry = NULL;
	List *copies = NIL;

	/* copy first: resolving column names may access the catalogs */
	LWLockAcquire(ObsTableLock(OBS_TABLE_COLUMN), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_COLUMN));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsColumnEntry *copy = palloc(sizeof(ObsColumnEntry));

		SpinLockAcquire(&entry->mutex);
		*copy = *entry;
		SpinLockRelease(&entry->mutex);
		copies = lappend(copies, copy);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_COLUMN));

	ObsColumnEntry *copy = NULL;
	foreach_declared_ptr(copy, copies)
	{
		char *columnName = get_attname(copy->key.relationId, copy->key.attnum, true);
		Datum values[6];
		bool isNulls[6] = { false };

		if (columnName == NULL)
		{
			/* the table or column was dropped */
			continue;
		}

		values[0] = Int64GetDatum((int64) copy->key.queryId);
		values[1] = ObjectIdGetDatum(copy->key.relationId);
		values[2] = CStringGetTextDatum(columnName);
		values[3] = CStringGetTextDatum(ObsUsageNames[copy->key.usage]);
		values[4] = BoolGetDatum(copy->isDistributionColumn);
		values[5] = Int32GetDatum(Max(copy->occurrences, 1));

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}


/*
 * citus_observer_join_edges returns the join edge table.
 */
Datum
citus_observer_join_edges(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	HASH_SEQ_STATUS status;
	ObsJoinEntry *entry = NULL;
	List *copies = NIL;

	LWLockAcquire(ObsTableLock(OBS_TABLE_JOIN), LW_SHARED);
	hash_seq_init(&status, ObsTableHash(OBS_TABLE_JOIN));
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		ObsJoinEntry *copy = palloc(sizeof(ObsJoinEntry));

		SpinLockAcquire(&entry->mutex);
		*copy = *entry;
		SpinLockRelease(&entry->mutex);
		copies = lappend(copies, copy);
	}
	LWLockRelease(ObsTableLock(OBS_TABLE_JOIN));

	ObsJoinEntry *copy = NULL;
	foreach_declared_ptr(copy, copies)
	{
		char *leftColumn = get_attname(copy->key.leftRelationId, copy->key.leftAttnum,
									   true);
		char *rightColumn = get_attname(copy->key.rightRelationId,
										copy->key.rightAttnum, true);
		Datum values[7];
		bool isNulls[7] = { false };

		if (leftColumn == NULL || rightColumn == NULL)
		{
			continue;
		}

		values[0] = Int64GetDatum((int64) copy->key.queryId);
		values[1] = ObjectIdGetDatum(copy->key.leftRelationId);
		values[2] = CStringGetTextDatum(leftColumn);
		values[3] = ObjectIdGetDatum(copy->key.rightRelationId);
		values[4] = CStringGetTextDatum(rightColumn);
		values[5] = BoolGetDatum(copy->colocated);
		values[6] = BoolGetDatum(copy->pushdown);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}
