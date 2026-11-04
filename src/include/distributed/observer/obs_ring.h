/*-------------------------------------------------------------------------
 *
 * obs_ring.h
 *	  Sequenced event ring buffer for discrete observer events, such as
 *	  distributed transaction traces and shard move/split phase changes.
 *	  Writers never block on readers: when the ring is full
 *	  the oldest events are overwritten and readers detect the gap from the
 *	  sequence numbers.
 *
 * Plain C99, no PostgreSQL headers. The caller provides locking: exclusive
 * for obs_ring_push(), shared for the readers.
 *
 *-------------------------------------------------------------------------
 */
#ifndef OBS_RING_H
#define OBS_RING_H

#include <stddef.h>
#include <stdint.h>

#define OBS_RING_KIND_LEN 24
#define OBS_RING_PAYLOAD_LEN 1000

typedef struct ObsRingSlot
{
	uint64_t seq;               /* 0 = never written */
	int64_t ts_us;              /* caller-defined timestamp (PG: TimestampTz) */
	char kind[OBS_RING_KIND_LEN];
	uint32_t len;               /* payload bytes, excluding NUL */
	char payload[OBS_RING_PAYLOAD_LEN];     /* NUL-terminated, truncated */
} ObsRingSlot;

typedef struct ObsRing
{
	uint32_t nslots;
	uint64_t next_seq;          /* seq the next push gets; starts at 1 */
	ObsRingSlot slots[];
} ObsRing;

extern size_t obs_ring_size(uint32_t nslots);
extern void obs_ring_init(ObsRing *r, uint32_t nslots);

/*
 * Append an event and return its sequence number. Payloads longer than
 * OBS_RING_PAYLOAD_LEN - 1 are truncated (the slot's len reflects the
 * stored length).
 */
extern uint64_t obs_ring_push(ObsRing *r, int64_t ts_us, const char *kind,
							  const char *payload, size_t len);

/* newest assigned seq, 0 if nothing was pushed */
extern uint64_t obs_ring_head(const ObsRing *r);

/* oldest seq still stored, 0 if empty */
extern uint64_t obs_ring_oldest(const ObsRing *r);

/* slot holding seq, or NULL if seq was overwritten or not yet written */
extern const ObsRingSlot *obs_ring_get(const ObsRing *r, uint64_t seq);

/*
 * For a reader that has consumed everything up to and including `since`,
 * return the first seq it should read next (> since) and set *lost to how
 * many events in between were overwritten before it could read them.
 * Returns 0 if there is nothing newer than since.
 */
extern uint64_t obs_ring_next(const ObsRing *r, uint64_t since, uint64_t *lost);

#endif /* OBS_RING_H */
