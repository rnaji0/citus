/*-------------------------------------------------------------------------
 *
 * action_trace.c
 *	  Phase timings of shard moves, copies and splits, emitted as events.
 *
 * A move or copy reports every progress status change (setting up,
 * copying data, catching up, ... completed) through the rebalance progress
 * monitor. Each change emits an "action_phase" event with the duration of
 * the phase that just ended; completion emits an "action_end" event with
 * the total time, the time of every phase and the bytes moved. Splits and
 * tenant isolation emit only "action_end". An action interrupted by an
 * error emits "action_end" with outcome "failed".
 *
 * These measured costs are what a controller needs to decide whether a
 * move is worth it.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "lib/stringinfo.h"
#include "utils/json.h"
#include "utils/timestamp.h"

#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/observer/observer.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_transfer.h"

#define MAX_ACTION_SHARDS 32
#define ACTION_PHASE_COUNT (PLACEMENT_UPDATE_STATUS_COMPLETED + 1)

static const char *const PhaseNames[ACTION_PHASE_COUNT] = {
	[PLACEMENT_UPDATE_STATUS_NOT_STARTED_YET] = "not_started",
	[PLACEMENT_UPDATE_STATUS_SETTING_UP] = "setting_up",
	[PLACEMENT_UPDATE_STATUS_COPYING_DATA] = "copying_data",
	[PLACEMENT_UPDATE_STATUS_CATCHING_UP] = "catching_up",
	[PLACEMENT_UPDATE_STATUS_CREATING_CONSTRAINTS] = "creating_constraints",
	[PLACEMENT_UPDATE_STATUS_FINAL_CATCH_UP] = "final_catch_up",
	[PLACEMENT_UPDATE_STATUS_CREATING_FOREIGN_KEYS] = "creating_foreign_keys",
	[PLACEMENT_UPDATE_STATUS_COMPLETING] = "completing",
	[PLACEMENT_UPDATE_STATUS_COMPLETED] = "completed",
};

typedef struct ActiveAction
{
	bool active;
	uint64 actionId;
	char operation[32];
	char mode[32];
	char source[NAMEDATALEN + 16];
	char target[NAMEDATALEN + 16];
	int shardCount;
	uint64 shardIds[MAX_ACTION_SHARDS];
	int phase;                  /* PlacementUpdateStatus, -1 if not tracked */
	TimestampTz startTime;
	TimestampTz phaseStartTime;
	double phaseMs[ACTION_PHASE_COUNT];
	uint64 bytes;
} ActiveAction;

static ActiveAction Action;
static uint32 ActionCounter = 0;

static void StartAction(const char *operation, const char *mode);
static void AppendShardList(StringInfo buf);
static double MillisecondsSince(TimestampTz start, TimestampTz now);


/*
 * MillisecondsSince returns now - start in milliseconds.
 */
static double
MillisecondsSince(TimestampTz start, TimestampTz now)
{
	return (double) (now - start) / 1000.0;
}


/*
 * StartAction resets the backend's action state for a new operation.
 */
static void
StartAction(const char *operation, const char *mode)
{
	memset(&Action, 0, sizeof(Action));
	Action.active = true;
	Action.actionId = ((uint64) MyProcPid << 32) | ++ActionCounter;
	strlcpy(Action.operation, operation, sizeof(Action.operation));
	strlcpy(Action.mode, mode != NULL ? mode : "", sizeof(Action.mode));
	Action.phase = -1;
	Action.startTime = GetCurrentTimestamp();
	Action.phaseStartTime = Action.startTime;
}


/*
 * AppendShardList appends the action's shard ids as a JSON array.
 */
static void
AppendShardList(StringInfo buf)
{
	appendStringInfoChar(buf, '[');
	for (int i = 0; i < Action.shardCount; i++)
	{
		appendStringInfo(buf, "%s" UINT64_FORMAT, i > 0 ? ", " : "",
						 Action.shardIds[i]);
	}
	appendStringInfoChar(buf, ']');
}


/*
 * ObserverShardTransferBegin starts tracking a shard move or copy of a
 * colocated shard group.
 */
void
ObserverShardTransferBegin(const char *operation, List *shardIntervalList,
						   const char *sourceName, int sourcePort,
						   const char *targetName, int targetPort)
{
	if (!ObserverAreaEnabled(OBS_AREA_ACTION))
	{
		Action.active = false;
		return;
	}

	StartAction(operation, NULL);
	snprintf(Action.source, sizeof(Action.source), "%s:%d", sourceName, sourcePort);
	snprintf(Action.target, sizeof(Action.target), "%s:%d", targetName, targetPort);

	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, shardIntervalList)
	{
		if (Action.shardCount < MAX_ACTION_SHARDS)
		{
			Action.shardIds[Action.shardCount++] = shardInterval->shardId;
		}
	}

	Action.bytes = ShardListSizeInBytes(shardIntervalList, (char *) sourceName,
										sourcePort);
}


/*
 * ObserverShardTransferStatus is called whenever a move or copy reports a
 * new progress status for its shards.
 */
void
ObserverShardTransferStatus(List *shardIntervalList, int status)
{
	if (!Action.active || shardIntervalList == NIL || status < 0 ||
		status >= ACTION_PHASE_COUNT || status == Action.phase)
	{
		return;
	}

	ShardInterval *firstShard = linitial(shardIntervalList);
	bool belongsToAction = false;
	for (int i = 0; i < Action.shardCount; i++)
	{
		if (Action.shardIds[i] == firstShard->shardId)
		{
			belongsToAction = true;
			break;
		}
	}
	if (!belongsToAction)
	{
		return;
	}

	TimestampTz now = GetCurrentTimestamp();
	double previousMs = MillisecondsSince(Action.phaseStartTime, now);
	int previous = Action.phase;

	if (previous >= 0)
	{
		Action.phaseMs[previous] += previousMs;
	}

	StringInfoData payload;

	initStringInfo(&payload);
	appendStringInfo(&payload, "{\"action_id\": " UINT64_FORMAT ", \"operation\": ",
					 Action.actionId);
	escape_json(&payload, Action.operation);
	appendStringInfoString(&payload, ", \"shards\": ");
	AppendShardList(&payload);
	appendStringInfoString(&payload, ", \"source\": ");
	escape_json(&payload, Action.source);
	appendStringInfoString(&payload, ", \"target\": ");
	escape_json(&payload, Action.target);
	appendStringInfo(&payload, ", \"phase\": \"%s\", \"previous_phase\": ",
					 PhaseNames[status]);
	if (previous >= 0)
	{
		appendStringInfo(&payload, "\"%s\", \"previous_phase_ms\": %.3f}",
						 PhaseNames[previous], previousMs);
	}
	else
	{
		appendStringInfoString(&payload, "null, \"previous_phase_ms\": null}");
	}
	ObserverPushEvent("action_phase", payload.data, payload.len);
	pfree(payload.data);

	Action.phase = status;
	Action.phaseStartTime = now;

	if (status == PLACEMENT_UPDATE_STATUS_COMPLETED)
	{
		ObserverActionEnd(true);
	}
}


/*
 * ObserverSplitBegin starts tracking a split (or tenant isolation) of a
 * colocated shard group.
 */
void
ObserverSplitBegin(const char *operation, List *shardIntervalList, const char *mode)
{
	if (!ObserverAreaEnabled(OBS_AREA_ACTION) || shardIntervalList == NIL)
	{
		Action.active = false;
		return;
	}

	StartAction(operation, mode);

	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, shardIntervalList)
	{
		if (Action.shardCount < MAX_ACTION_SHARDS)
		{
			Action.shardIds[Action.shardCount++] = shardInterval->shardId;
		}
	}

	ShardInterval *firstShard = linitial(shardIntervalList);
	ShardPlacement *placement = ActiveShardPlacement(firstShard->shardId, false);
	snprintf(Action.source, sizeof(Action.source), "%s:%d", placement->nodeName,
			 placement->nodePort);
	Action.bytes = ShardListSizeInBytes(shardIntervalList, placement->nodeName,
										placement->nodePort);
}


/*
 * ObserverActionEnd emits the action_end event of the tracked action.
 */
void
ObserverActionEnd(bool succeeded)
{
	if (!Action.active)
	{
		return;
	}

	TimestampTz now = GetCurrentTimestamp();
	bool first = true;
	StringInfoData payload;

	if (Action.phase >= 0 && Action.phase != PLACEMENT_UPDATE_STATUS_COMPLETED)
	{
		Action.phaseMs[Action.phase] += MillisecondsSince(Action.phaseStartTime, now);
	}

	initStringInfo(&payload);
	appendStringInfo(&payload, "{\"action_id\": " UINT64_FORMAT ", \"operation\": ",
					 Action.actionId);
	escape_json(&payload, Action.operation);
	appendStringInfoString(&payload, ", \"shards\": ");
	AppendShardList(&payload);
	if (Action.source[0] != '\0')
	{
		appendStringInfoString(&payload, ", \"source\": ");
		escape_json(&payload, Action.source);
	}
	if (Action.target[0] != '\0')
	{
		appendStringInfoString(&payload, ", \"target\": ");
		escape_json(&payload, Action.target);
	}
	if (Action.mode[0] != '\0')
	{
		appendStringInfoString(&payload, ", \"mode\": ");
		escape_json(&payload, Action.mode);
	}
	appendStringInfo(&payload, ", \"outcome\": \"%s\", \"total_ms\": %.3f, \"phases\": {",
					 succeeded ? "completed" : "failed",
					 MillisecondsSince(Action.startTime, now));
	for (int phase = 0; phase < ACTION_PHASE_COUNT; phase++)
	{
		if (Action.phaseMs[phase] > 0)
		{
			appendStringInfo(&payload, "%s\"%s\": %.3f", first ? "" : ", ",
							 PhaseNames[phase], Action.phaseMs[phase]);
			first = false;
		}
	}
	if (Action.phase < 0)
	{
		/* splits do not report progress statuses; report the whole operation */
		appendStringInfoString(&payload, first ? "" : ", ");
		escape_json(&payload, Action.operation);
		appendStringInfo(&payload, ": %.3f", MillisecondsSince(Action.startTime, now));
	}
	appendStringInfo(&payload, "}, \"bytes\": " UINT64_FORMAT "}", Action.bytes);

	ObserverPushEvent("action_end", payload.data, payload.len);
	pfree(payload.data);

	Action.active = false;
}


/*
 * ObserverActionAbort reports a tracked action interrupted by an error.
 * It runs in the abort callback, where the memory context is reset, so it
 * must only be cheap.
 */
void
ObserverActionAbort(void)
{
	if (Action.active)
	{
		ObserverActionEnd(false);
	}
}
