/*-------------------------------------------------------------------------
 *
 * obs_ring.c
 *	  Sequenced, overwrite-oldest event ring buffer. See obs_ring.h.
 *
 * Event seq numbers start at 1 and increase by one per push. The event
 * with seq s lives in slot (s - 1) % nslots, so a reader can tell whether
 * an event is still stored by comparing the slot's seq with the one it
 * asked for.
 *
 * Plain C99; no PostgreSQL dependencies.
 *
 *-------------------------------------------------------------------------
 */
#include <string.h>

#include "distributed/observer/obs_ring.h"


/*
 * obs_ring_size returns the number of bytes needed for a ring with nslots
 * slots.
 */
size_t
obs_ring_size(uint32_t nslots)
{
	return offsetof(ObsRing, slots) + (size_t) (nslots < 1 ? 1 : nslots) *
		   sizeof(ObsRingSlot);
}


/*
 * obs_ring_init sets up an empty ring in memory of at least
 * obs_ring_size(nslots) bytes.
 */
void
obs_ring_init(ObsRing *ring, uint32_t nslots)
{
	ring->nslots = nslots < 1 ? 1 : nslots;
	ring->next_seq = 1;
	memset(ring->slots, 0, (size_t) ring->nslots * sizeof(ObsRingSlot));
}


/*
 * obs_ring_push appends an event, overwriting the oldest one if the ring
 * is full, and returns the event's seq. The caller must hold an exclusive
 * lock.
 */
uint64_t
obs_ring_push(ObsRing *ring, int64_t ts_us, const char *kind,
			  const char *payload, size_t len)
{
	uint64_t seq = ring->next_seq++;
	ObsRingSlot *slot = &ring->slots[(seq - 1) % ring->nslots];

	if (payload == NULL)
	{
		len = 0;
	}
	if (len > OBS_RING_PAYLOAD_LEN - 1)
	{
		len = OBS_RING_PAYLOAD_LEN - 1;
	}

	slot->seq = seq;
	slot->ts_us = ts_us;
	strncpy(slot->kind, kind ? kind : "", OBS_RING_KIND_LEN - 1);
	slot->kind[OBS_RING_KIND_LEN - 1] = '\0';
	if (len > 0)
	{
		memcpy(slot->payload, payload, len);
	}
	slot->payload[len] = '\0';
	slot->len = (uint32_t) len;
	return seq;
}


/*
 * obs_ring_head returns the newest seq, or 0 if nothing was pushed.
 */
uint64_t
obs_ring_head(const ObsRing *ring)
{
	return ring->next_seq - 1;
}


/*
 * obs_ring_oldest returns the oldest seq still stored, or 0 if the ring
 * is empty.
 */
uint64_t
obs_ring_oldest(const ObsRing *ring)
{
	uint64_t head = obs_ring_head(ring);

	if (head == 0)
	{
		return 0;
	}
	return head > ring->nslots ? head - ring->nslots + 1 : 1;
}


/*
 * obs_ring_get returns the slot holding seq, or NULL if seq has not been
 * written yet or was already overwritten.
 */
const ObsRingSlot *
obs_ring_get(const ObsRing *ring, uint64_t seq)
{
	if (seq == 0 || seq >= ring->next_seq)
	{
		return NULL;
	}

	const ObsRingSlot *slot = &ring->slots[(seq - 1) % ring->nslots];

	return slot->seq == seq ? slot : NULL;
}


/*
 * obs_ring_next returns the first seq a reader that has consumed
 * everything up to since should read next, and sets *lost to the number
 * of events after since that were overwritten before they could be read.
 * Returns 0 when there is nothing newer than since.
 */
uint64_t
obs_ring_next(const ObsRing *ring, uint64_t since, uint64_t *lost)
{
	uint64_t head = obs_ring_head(ring);
	uint64_t oldest = obs_ring_oldest(ring);

	if (lost != NULL)
	{
		*lost = 0;
	}
	if (head == 0 || since >= head)
	{
		return 0;
	}
	if (since + 1 < oldest)
	{
		if (lost != NULL)
		{
			*lost = oldest - (since + 1);
		}
		return oldest;
	}
	return since + 1;
}
