/*-------------------------------------------------------------------------
 *
 * citus_observer_decoder.c
 *	  Logical decoding output plugin that summarizes row changes per shard
 *	  and distribution column value.
 *
 * Citus metadata cannot be read while decoding, so the consumer passes a
 * map from local shard relations to their distribution column with the
 * "dist_columns" option, formatted as "relid:attnum:shardid;...". Changes
 * to relations outside the map are ignored.
 *
 * Within a transaction, inserted, updated and deleted rows and tuple bytes
 * are accumulated per (shard, value); at commit one line per pair is
 * written:
 *
 *	shardid TAB value TAB inserts TAB updates TAB deletes TAB bytes
 *
 * where value is \N for NULL. Changes replayed through a replication
 * origin are skipped; Citus sets an origin while it copies data for shard
 * moves and splits (with citus.enable_change_data_capture), so internal
 * data movement does not count as growth.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

#include "access/htup_details.h"
#include "replication/logical.h"
#include "replication/origin.h"
#include "replication/output_plugin.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

#define VALUE_LEN 64

typedef struct ShardColumn
{
	Oid relationId;
	AttrNumber attnum;
	uint64 shardId;
} ShardColumn;

typedef struct ChangeKey
{
	uint64 shardId;
	char value[VALUE_LEN];
} ChangeKey;

typedef struct ChangeCounts
{
	ChangeKey key;
	bool isNull;
	uint64 inserts;
	uint64 updates;
	uint64 deletes;
	uint64 bytes;
} ChangeCounts;

typedef struct DecoderState
{
	HTAB *shardColumns;
	MemoryContext transactionContext;
	HTAB *changes;              /* current transaction, NULL until first change */
} DecoderState;

extern void _PG_output_plugin_init(OutputPluginCallbacks *callbacks);

static void DecoderStartup(LogicalDecodingContext *ctx, OutputPluginOptions *options,
						   bool isInit);
static void DecoderShutdown(LogicalDecodingContext *ctx);
static void DecoderBegin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void DecoderChange(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  Relation relation, ReorderBufferChange *change);
static void DecoderCommit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  XLogRecPtr commitLsn);
static bool DecoderFilterByOrigin(LogicalDecodingContext *ctx, RepOriginId originId);
static void ParseShardColumns(DecoderState *state, const char *map);


/*
 * _PG_output_plugin_init registers the plugin's callbacks.
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *callbacks)
{
	callbacks->startup_cb = DecoderStartup;
	callbacks->begin_cb = DecoderBegin;
	callbacks->change_cb = DecoderChange;
	callbacks->commit_cb = DecoderCommit;
	callbacks->filter_by_origin_cb = DecoderFilterByOrigin;
	callbacks->shutdown_cb = DecoderShutdown;
}


/*
 * ParseShardColumns reads the "relid:attnum:shardid;..." map.
 */
static void
ParseShardColumns(DecoderState *state, const char *map)
{
	const char *cursor = map;

	while (cursor != NULL && *cursor != '\0')
	{
		unsigned int relationId = 0;
		int attnum = 0;
		unsigned long long shardId = 0;
		bool found = false;

		if (sscanf(cursor, "%u:%d:%llu", &relationId, &attnum, &shardId) == 3 &&
			attnum > 0)
		{
			Oid key = (Oid) relationId;
			ShardColumn *entry = hash_search(state->shardColumns, &key, HASH_ENTER,
											 &found);

			entry->attnum = (AttrNumber) attnum;
			entry->shardId = (uint64) shardId;
		}

		cursor = strchr(cursor, ';');
		if (cursor != NULL)
		{
			cursor++;
		}
	}
}


/*
 * DecoderStartup parses options and sets up memory.
 */
static void
DecoderStartup(LogicalDecodingContext *ctx, OutputPluginOptions *options, bool isInit)
{
	DecoderState *state = MemoryContextAllocZero(ctx->context, sizeof(DecoderState));
	HASHCTL info;
	ListCell *optionCell = NULL;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(ShardColumn);
	info.hcxt = ctx->context;
	state->shardColumns = hash_create("citus_observer_decoder shard columns", 256,
									  &info, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	state->transactionContext = AllocSetContextCreate(ctx->context,
													  "citus_observer_decoder txn",
													  ALLOCSET_DEFAULT_SIZES);

	foreach(optionCell, ctx->output_plugin_options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, "dist_columns") == 0)
		{
			if (option->arg != NULL)
			{
				ParseShardColumns(state, strVal(option->arg));
			}
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("option \"%s\" is not recognized by "
								   "citus_observer_decoder", option->defname)));
		}
	}

	options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
	options->receive_rewrites = false;
	ctx->output_plugin_private = state;
}


/*
 * DecoderShutdown has nothing to free beyond ctx->context.
 */
static void
DecoderShutdown(LogicalDecodingContext *ctx)
{
}


/*
 * DecoderBegin starts an empty summary for a transaction.
 */
static void
DecoderBegin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	DecoderState *state = ctx->output_plugin_private;

	MemoryContextReset(state->transactionContext);
	state->changes = NULL;
}


/*
 * DecoderFilterByOrigin skips changes replayed through a replication
 * origin.
 */
static bool
DecoderFilterByOrigin(LogicalDecodingContext *ctx, RepOriginId originId)
{
	return originId != InvalidRepOriginId;
}


/*
 * DecoderChange counts one row change of a mapped shard.
 */
static void
DecoderChange(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation,
			  ReorderBufferChange *change)
{
	DecoderState *state = ctx->output_plugin_private;
	Oid relationId = RelationGetRelid(relation);
	ShardColumn *shardColumn = hash_search(state->shardColumns, &relationId, HASH_FIND,
										   NULL);
	HeapTuple tuple = NULL;

	if (shardColumn == NULL)
	{
		return;
	}

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
		case REORDER_BUFFER_CHANGE_UPDATE:
		{
			tuple = change->data.tp.newtuple;
			break;
		}

		case REORDER_BUFFER_CHANGE_DELETE:
		{
			/* the old tuple carries the replica identity, which includes the key */
			tuple = change->data.tp.oldtuple;
			break;
		}

		default:
		{
			return;
		}
	}

	MemoryContext oldContext = MemoryContextSwitchTo(state->transactionContext);

	if (state->changes == NULL)
	{
		HASHCTL info;

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(ChangeKey);
		info.entrysize = sizeof(ChangeCounts);
		info.hcxt = state->transactionContext;
		state->changes = hash_create("citus_observer_decoder changes", 64, &info,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	ChangeKey key;
	bool isNull = true;

	memset(&key, 0, sizeof(key));
	key.shardId = shardColumn->shardId;

	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	if (tuple != NULL && shardColumn->attnum <= tupleDescriptor->natts)
	{
		Datum value = heap_getattr(tuple, shardColumn->attnum, tupleDescriptor, &isNull);

		if (!isNull)
		{
			Form_pg_attribute attribute = TupleDescAttr(tupleDescriptor,
														shardColumn->attnum - 1);
			Oid outputFunction = InvalidOid;
			bool isVarlena = false;

			getTypeOutputInfo(attribute->atttypid, &outputFunction, &isVarlena);
			strlcpy(key.value, OidOutputFunctionCall(outputFunction, value), VALUE_LEN);
		}
	}

	bool found = false;
	ChangeCounts *counts = hash_search(state->changes, &key, HASH_ENTER, &found);
	if (!found)
	{
		counts->isNull = isNull;
		counts->inserts = 0;
		counts->updates = 0;
		counts->deletes = 0;
		counts->bytes = 0;
	}

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
		{
			counts->inserts++;
			break;
		}

		case REORDER_BUFFER_CHANGE_UPDATE:
		{
			counts->updates++;
			break;
		}

		default:
		{
			counts->deletes++;
			break;
		}
	}
	if (tuple != NULL)
	{
		counts->bytes += tuple->t_len;
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * DecoderCommit writes the transaction's summary, one line per shard and
 * value.
 */
static void
DecoderCommit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commitLsn)
{
	DecoderState *state = ctx->output_plugin_private;
	HASH_SEQ_STATUS status;
	ChangeCounts *counts = NULL;

	if (state->changes == NULL)
	{
		return;
	}

	hash_seq_init(&status, state->changes);
	while ((counts = hash_seq_search(&status)) != NULL)
	{
		OutputPluginPrepareWrite(ctx, true);
		appendStringInfo(ctx->out, UINT64_FORMAT "\t%s\t" UINT64_FORMAT "\t" UINT64_FORMAT
						 "\t" UINT64_FORMAT "\t" UINT64_FORMAT,
						 counts->key.shardId, counts->isNull ? "\\N" : counts->key.value,
						 counts->inserts, counts->updates, counts->deletes,
						 counts->bytes);
		OutputPluginWrite(ctx, true);
	}

	MemoryContextReset(state->transactionContext);
	state->changes = NULL;
}
