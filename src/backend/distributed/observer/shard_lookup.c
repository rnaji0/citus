/*-------------------------------------------------------------------------
 *
 * shard_lookup.c
 *	  Backend-local cache that maps a local relation to the Citus shard it
 *	  stores, so executor hooks can attribute work to shards cheaply.
 *
 * A relation is a shard if its name is "<table name>_<shard id>" for a
 * shard id known to the metadata cache that belongs to <table name>. The
 * answer, including "not a shard", is cached per relation and dropped on
 * relcache invalidation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "distributed/metadata_cache.h"
#include "distributed/observer/observer.h"
#include "distributed/worker_protocol.h"

typedef struct RelationShardEntry
{
	Oid relationId;
	uint64 shardId;             /* INVALID_SHARD_ID if not a shard */
} RelationShardEntry;

static HTAB *RelationShardCache = NULL;

static void InvalidateRelationShardCache(Datum argument, Oid relationId);
static uint64 ComputeShardIdForRelation(Oid relationId);


/*
 * InitializeObserverShardLookup registers the relcache callback that keeps
 * the cache current.
 */
void
InitializeObserverShardLookup(void)
{
	CacheRegisterRelcacheCallback(InvalidateRelationShardCache, (Datum) 0);
}


/*
 * InvalidateRelationShardCache forgets one relation, or every relation
 * when relationId is invalid.
 */
static void
InvalidateRelationShardCache(Datum argument, Oid relationId)
{
	if (RelationShardCache == NULL)
	{
		return;
	}

	if (!OidIsValid(relationId))
	{
		hash_destroy(RelationShardCache);
		RelationShardCache = NULL;
		return;
	}

	hash_search(RelationShardCache, &relationId, HASH_REMOVE, NULL);
}


/*
 * ObserverShardIdForRelation returns the shard id stored in a local
 * relation, or INVALID_SHARD_ID if the relation is not a shard. Must be
 * called inside a transaction.
 */
uint64
ObserverShardIdForRelation(Oid relationId)
{
	bool found = false;

	if (!OidIsValid(relationId) || !IsTransactionState() || !CitusHasBeenLoaded())
	{
		return INVALID_SHARD_ID;
	}

	if (RelationShardCache == NULL)
	{
		HASHCTL info;

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(Oid);
		info.entrysize = sizeof(RelationShardEntry);
		info.hcxt = CacheMemoryContext;
		RelationShardCache = hash_create("citus_observer relation shards", 256, &info,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	RelationShardEntry *entry = hash_search(RelationShardCache, &relationId,
											HASH_FIND, &found);
	if (found)
	{
		return entry->shardId;
	}

	uint64 shardId = ComputeShardIdForRelation(relationId);

	/* computing may have processed invalidations that dropped the cache */
	if (RelationShardCache != NULL)
	{
		entry = hash_search(RelationShardCache, &relationId, HASH_ENTER, &found);
		entry->shardId = shardId;
	}
	return shardId;
}


/*
 * ComputeShardIdForRelation checks a relation's name against the metadata
 * cache.
 */
static uint64
ComputeShardIdForRelation(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	Oid distributedRelationId = InvalidOid;
	uint32 colocationId = 0;
	int shardIndex = 0;
	bool isHashDistributed = false;

	if (relationName == NULL)
	{
		return INVALID_SHARD_ID;
	}

	uint64 shardId = ExtractShardIdFromTableName(relationName, true);
	if (shardId == INVALID_SHARD_ID)
	{
		return INVALID_SHARD_ID;
	}

	if (!LookupShardPosition(shardId, &distributedRelationId, &colocationId,
							 &shardIndex, &isHashDistributed))
	{
		return INVALID_SHARD_ID;
	}

	/* the shard must also be stored under its table's name */
	char *distributedRelationName = get_rel_name(distributedRelationId);
	if (distributedRelationName == NULL ||
		strncmp(relationName, distributedRelationName,
				strlen(distributedRelationName)) != 0)
	{
		return INVALID_SHARD_ID;
	}

	return shardId;
}
