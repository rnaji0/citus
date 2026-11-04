/*-------------------------------------------------------------------------
 *
 * observer_tables.h
 *	  Entry layouts of the observer's shared hash tables.
 *
 * Every entry starts with its key, followed by a spinlock that protects the
 * counters after it. Keys are compared as raw bytes, so callers must zero a
 * key (including padding) before filling it in.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CITUS_OBSERVER_TABLES_H
#define CITUS_OBSERVER_TABLES_H

#include "postgres.h"

#include "storage/spin.h"

#include "distributed/observer/obs_sketch.h"

#define OBS_REASON_LEN 256
#define OBS_TENANT_LEN 64
#define OBS_ROW_KEY_LEN OBS_SKETCH_KEY_LEN

/* tasks sent to a shard placement, by command */
typedef struct ObsTaskKey
{
	uint64 shardId;
	int32 nodeId;
	int32 command;              /* ObsCommand */
} ObsTaskKey;

typedef struct ObsTaskEntry
{
	ObsTaskKey key;
	slock_t mutex;
	int32 colocationId;
	uint64 calls;
	uint64 failures;
	uint64 localCalls;
	uint64 rows;
	uint64 bytes;
	double totalUs;
	double maxUs;
} ObsTaskEntry;

/* resources used by statements on a local shard */
typedef struct ObsResourceKey
{
	uint64 shardId;
} ObsResourceKey;

typedef struct ObsResourceEntry
{
	ObsResourceKey key;
	slock_t mutex;
	uint64 calls;
	uint64 writeCalls;
	double execMs;
	double userMs;
	double sysMs;
	double sharedBlksHit;
	double sharedBlksRead;
	double sharedBlksDirtied;
	double sharedBlksWritten;
	double tempBlksWritten;
	double walRecords;
	double walBytes;
	double rows;
} ObsResourceEntry;

/* plan decision and execution counts per query fingerprint */
typedef enum ObsPlanKind
{
	OBS_PLAN_FAST_PATH_ROUTER = 0,
	OBS_PLAN_ROUTER,
	OBS_PLAN_MULTI_SHARD,
	OBS_PLAN_REPARTITION,
	OBS_PLAN_INSERT_SELECT_PUSHDOWN,
	OBS_PLAN_INSERT_SELECT_REPARTITION,
	OBS_PLAN_INSERT_SELECT_COORDINATOR,
	OBS_PLAN_MERGE,
	OBS_PLAN_RECURSIVE,
	OBS_PLAN_KIND_COUNT
} ObsPlanKind;

typedef struct ObsPlanKey
{
	uint64 queryId;
	int32 planKind;             /* ObsPlanKind */
	int32 padding;
} ObsPlanKey;

typedef struct ObsPlanEntry
{
	ObsPlanKey key;
	slock_t mutex;
	int32 colocationId;
	int32 maxTasks;
	int32 subplans;
	uint64 calls;
	uint64 singleShardCalls;
	uint64 multiShardCalls;
	uint64 totalTasks;
	double totalMs;
	char reason[OBS_REASON_LEN];
} ObsPlanEntry;

/* how a query fingerprint uses a column of a distributed table */
typedef enum ObsColumnUsage
{
	OBS_USAGE_EQ = 0,
	OBS_USAGE_IN,
	OBS_USAGE_RANGE,
	OBS_USAGE_JOIN,
	OBS_USAGE_GROUP,
	OBS_USAGE_COUNT
} ObsColumnUsage;

typedef struct ObsColumnKey
{
	uint64 queryId;
	Oid relationId;
	int16 attnum;
	int16 usage;                /* ObsColumnUsage */
} ObsColumnKey;

typedef struct ObsColumnEntry
{
	ObsColumnKey key;
	slock_t mutex;
	bool isDistributionColumn;
	int32 occurrences;
} ObsColumnEntry;

/* equi-join condition between two tables */
typedef struct ObsJoinKey
{
	uint64 queryId;
	Oid leftRelationId;
	Oid rightRelationId;
	int16 leftAttnum;
	int16 rightAttnum;
	int32 padding;
} ObsJoinKey;

typedef struct ObsJoinEntry
{
	ObsJoinKey key;
	slock_t mutex;
	bool colocated;
	bool pushdown;
} ObsJoinEntry;

/* two shard groups accessed by one distributed transaction */
typedef struct ObsGroupPairKey
{
	int32 colocationId;
	int32 shardIndexA;
	int32 shardIndexB;
} ObsGroupPairKey;

typedef struct ObsGroupPairEntry
{
	ObsGroupPairKey key;
	slock_t mutex;
	uint64 txns;
	uint64 twoPhaseCommits;
} ObsGroupPairEntry;

/* two distribution column values accessed by one distributed transaction */
typedef struct ObsTenantPairKey
{
	int32 colocationId;
	char tenantA[OBS_TENANT_LEN];
	char tenantB[OBS_TENANT_LEN];
} ObsTenantPairKey;

typedef struct ObsTenantPairEntry
{
	ObsTenantPairKey key;
	slock_t mutex;
	uint64 txns;
} ObsTenantPairEntry;

/* sampled lock waits */
typedef struct ObsContentionKey
{
	Oid relationId;
	int16 lockType;             /* LockTagType */
	int16 lockMode;             /* LOCKMODE waited for */
	char rowKey[OBS_ROW_KEY_LEN];
} ObsContentionKey;

typedef struct ObsContentionEntry
{
	ObsContentionKey key;
	slock_t mutex;
	uint64 shardId;
	uint64 samples;
} ObsContentionEntry;

#endif /* CITUS_OBSERVER_TABLES_H */
