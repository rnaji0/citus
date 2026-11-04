/*-------------------------------------------------------------------------
 *
 * obs_sketch.h
 *	  Heavy-hitter sketch used to find the most frequently accessed row
 *	  keys per shard.
 *
 * A count-min sketch estimates the frequency of every key in bounded
 * memory; a small top-K table keeps the keys whose estimate is largest.
 * Counter increments are lock-free (atomic adds), so the common case of a
 * cold key never takes a lock. Only when a key's estimate beats the current
 * top-K minimum does the caller take its lock and call obs_sketch_offer().
 *
 * This file is plain C99 (no PostgreSQL headers) so the sketch can be unit
 * tested outside the server. The struct is position independent and lives
 * directly in shared memory.
 *
 *-------------------------------------------------------------------------
 */
#ifndef OBS_SKETCH_H
#define OBS_SKETCH_H

#include <stddef.h>
#include <stdint.h>

#define OBS_SKETCH_KEY_LEN 96

typedef enum ObsAccessKind
{
	OBS_ACCESS_READ = 0,
	OBS_ACCESS_WRITE = 1
} ObsAccessKind;

typedef struct ObsTopKEntry
{
	uint64_t hash;          /* 0 means the slot is empty */
	uint64_t count;         /* estimated occurrences (upper bound) */
	uint64_t error;         /* count the entry inherited when it was admitted */
	uint64_t shardid;
	uint8_t access;         /* ObsAccessKind */
	char key[OBS_SKETCH_KEY_LEN];   /* NUL-terminated, truncated */
} ObsTopKEntry;

typedef struct ObsSketch
{
	uint32_t depth;
	uint32_t width;         /* power of two */
	uint32_t k;
	uint32_t used;          /* occupied top-K slots */
	uint64_t total;         /* increments since init/reset (atomic) */
	uint64_t min_count;     /* cached min top-K count, 0 until full (atomic) */

	/*
	 * Followed in memory by depth * width uint32 counters and then k
	 * ObsTopKEntry slots. Use the accessors below.
	 */
	uint64_t data[];
} ObsSketch;

/* bytes needed for a sketch with these dimensions; width is rounded up to 2^n */
extern size_t obs_sketch_size(uint32_t depth, uint32_t width, uint32_t k);
extern void obs_sketch_init(ObsSketch *s, uint32_t depth, uint32_t width, uint32_t k);
extern void obs_sketch_reset(ObsSketch *s);

/* 64-bit hash of an arbitrary byte string (FNV-1a + murmur finalizer) */
extern uint64_t obs_hash64(const void *data, size_t len, uint64_t seed);

/*
 * Lock-free: add one occurrence of hash and return the new estimate
 * (minimum over the rows). Safe to call concurrently from many backends.
 */
extern uint64_t obs_sketch_increment(ObsSketch *s, uint64_t hash);
extern uint64_t obs_sketch_estimate(const ObsSketch *s, uint64_t hash);

/*
 * Returns true if a key with this estimate should be offered to the top-K
 * table: the table has room, or estimate exceeds the cached minimum.
 * Lock-free; a racy read only causes an extra or a skipped offer.
 */
extern int obs_sketch_wants(const ObsSketch *s, uint64_t estimate);

/*
 * Caller must hold an exclusive lock protecting the top-K table.
 * If hash is already present its count is raised to estimate. Otherwise it
 * is admitted into a free slot, or replaces the minimum entry when
 * estimate > minimum (error = replaced count). Returns 1 if the table
 * changed, 0 otherwise.
 */
extern int obs_sketch_offer(ObsSketch *s, uint64_t hash, uint64_t estimate,
							uint64_t shardid, uint8_t access, const char *key);

/*
 * Copy up to max occupied entries to out, sorted by count descending.
 * Caller holds at least a shared lock. Returns the number copied.
 */
extern int obs_sketch_topk(const ObsSketch *s, ObsTopKEntry *out, int max);

extern uint64_t obs_sketch_total(const ObsSketch *s);

#endif /* OBS_SKETCH_H */
