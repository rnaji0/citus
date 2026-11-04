/*-------------------------------------------------------------------------
 *
 * obs_sketch.c
 *	  Count-min sketch with a top-K table of heavy hitters.
 *
 * The sketch keeps depth rows of width counters. Every increment adds one
 * to a single counter per row, chosen by an independent hash of the key,
 * and the estimate for a key is the minimum over its counters. Estimates
 * only over-count, and the error is bounded by total / width with high
 * probability.
 *
 * Keys whose estimate is large enough are tracked by value in a small
 * top-K table, so that callers can report which keys are hot and not just
 * how hot a given key is.
 *
 * Plain C99 with GCC/Clang __atomic builtins; no PostgreSQL dependencies.
 *
 *-------------------------------------------------------------------------
 */
#include <string.h>

#include "distributed/observer/obs_sketch.h"


static uint32_t RoundUpToPowerOfTwo(uint32_t value);
static uint32_t * SketchCounters(const ObsSketch *sketch);
static ObsTopKEntry * SketchSlots(const ObsSketch *sketch);
static uint32_t SketchColumn(const ObsSketch *sketch, uint64_t hash, uint32_t row);
static void RefreshMinCount(ObsSketch *sketch);
static void FillEntry(ObsTopKEntry *entry, uint64_t hash, uint64_t count,
					  uint64_t error, uint64_t shardid, uint8_t access,
					  const char *key);


/*
 * RoundUpToPowerOfTwo returns the smallest power of two that is >= value,
 * capped at 2^30.
 */
static uint32_t
RoundUpToPowerOfTwo(uint32_t value)
{
	uint32_t power = 1;

	while (power < value && power < (1u << 30))
	{
		power <<= 1;
	}
	return power;
}


/*
 * SketchCounters returns the start of the depth * width counter matrix.
 */
static uint32_t *
SketchCounters(const ObsSketch *sketch)
{
	return (uint32_t *) sketch->data;
}


/*
 * SketchSlots returns the start of the top-K table, which follows the
 * counter matrix and is kept 8-byte aligned.
 */
static ObsTopKEntry *
SketchSlots(const ObsSketch *sketch)
{
	size_t counterBytes = (size_t) sketch->depth * sketch->width * sizeof(uint32_t);

	counterBytes = (counterBytes + 7) & ~(size_t) 7;
	return (ObsTopKEntry *) ((char *) sketch->data + counterBytes);
}


/*
 * obs_sketch_size returns the number of bytes needed for a sketch with the
 * given dimensions. width is rounded up to a power of two.
 */
size_t
obs_sketch_size(uint32_t depth, uint32_t width, uint32_t k)
{
	if (depth < 1)
	{
		depth = 1;
	}

	size_t counterBytes = (size_t) depth * RoundUpToPowerOfTwo(width < 1 ? 1 : width) *
						  sizeof(uint32_t);

	counterBytes = (counterBytes + 7) & ~(size_t) 7;
	return offsetof(ObsSketch, data) + counterBytes + (size_t) k * sizeof(ObsTopKEntry);
}


/*
 * obs_sketch_init sets up a sketch in memory of at least
 * obs_sketch_size(depth, width, k) bytes.
 */
void
obs_sketch_init(ObsSketch *sketch, uint32_t depth, uint32_t width, uint32_t k)
{
	sketch->depth = depth < 1 ? 1 : depth;
	sketch->width = RoundUpToPowerOfTwo(width < 1 ? 1 : width);
	sketch->k = k;
	obs_sketch_reset(sketch);
}


/*
 * obs_sketch_reset clears all counters and the top-K table. The caller
 * must hold an exclusive lock, since concurrent increments would be lost
 * or partially kept.
 */
void
obs_sketch_reset(ObsSketch *sketch)
{
	memset(SketchCounters(sketch), 0,
		   (size_t) sketch->depth * sketch->width * sizeof(uint32_t));
	memset(SketchSlots(sketch), 0, (size_t) sketch->k * sizeof(ObsTopKEntry));
	sketch->used = 0;
	__atomic_store_n(&sketch->total, 0, __ATOMIC_RELAXED);
	__atomic_store_n(&sketch->min_count, 0, __ATOMIC_RELAXED);
}


/*
 * obs_hash64 hashes a byte string with FNV-1a followed by the murmur3
 * 64-bit finalizer. It never returns 0, which marks empty top-K slots.
 */
uint64_t
obs_hash64(const void *data, size_t len, uint64_t seed)
{
	const unsigned char *bytes = data;
	uint64_t hash = 1469598103934665603ULL ^ seed;

	for (size_t i = 0; i < len; i++)
	{
		hash ^= bytes[i];
		hash *= 1099511628211ULL;
	}

	hash ^= hash >> 33;
	hash *= 0xff51afd7ed558ccdULL;
	hash ^= hash >> 33;
	hash *= 0xc4ceb9fe1a85ec53ULL;
	hash ^= hash >> 33;
	return hash == 0 ? 1 : hash;
}


/*
 * SketchColumn derives an independent column for the given row from the
 * key hash (splitmix64 of the hash offset by the row).
 */
static uint32_t
SketchColumn(const ObsSketch *sketch, uint64_t hash, uint32_t row)
{
	uint64_t mixed = hash + (uint64_t) row * 0x9e3779b97f4a7c15ULL;

	mixed ^= mixed >> 31;
	mixed *= 0xbf58476d1ce4e5b9ULL;
	mixed ^= mixed >> 29;
	return (uint32_t) mixed & (sketch->width - 1);
}


/*
 * obs_sketch_increment adds one occurrence of hash and returns the new
 * estimate. It only uses atomic adds and is safe to call concurrently.
 */
uint64_t
obs_sketch_increment(ObsSketch *sketch, uint64_t hash)
{
	uint32_t *counterMatrix = SketchCounters(sketch);
	uint64_t estimate = UINT64_MAX;

	for (uint32_t row = 0; row < sketch->depth; row++)
	{
		uint32_t *cell = &counterMatrix[(size_t) row * sketch->width +
										SketchColumn(sketch, hash, row)];
		uint32_t value = __atomic_add_fetch(cell, 1, __ATOMIC_RELAXED);

		if (value < estimate)
		{
			estimate = value;
		}
	}
	__atomic_add_fetch(&sketch->total, 1, __ATOMIC_RELAXED);
	return estimate;
}


/*
 * obs_sketch_estimate returns the current estimate for hash without
 * changing the sketch.
 */
uint64_t
obs_sketch_estimate(const ObsSketch *sketch, uint64_t hash)
{
	uint32_t *counterMatrix = SketchCounters(sketch);
	uint64_t estimate = UINT64_MAX;

	for (uint32_t row = 0; row < sketch->depth; row++)
	{
		uint32_t value = __atomic_load_n(&counterMatrix[(size_t) row * sketch->width +
														SketchColumn(sketch, hash, row)],
										 __ATOMIC_RELAXED);

		if (value < estimate)
		{
			estimate = value;
		}
	}
	return estimate == UINT64_MAX ? 0 : estimate;
}


/*
 * obs_sketch_wants returns whether a key with the given estimate is worth
 * offering to the top-K table: the table has room, or the estimate beats
 * the cached minimum. It reads without a lock, so a race only causes an
 * unnecessary offer or a skipped one that the next occurrence retries.
 */
int
obs_sketch_wants(const ObsSketch *sketch, uint64_t estimate)
{
	if (sketch->k == 0)
	{
		return 0;
	}
	if (__atomic_load_n(&sketch->used, __ATOMIC_RELAXED) < sketch->k)
	{
		return 1;
	}
	return estimate > __atomic_load_n(&sketch->min_count, __ATOMIC_RELAXED);
}


/*
 * RefreshMinCount recomputes the cached minimum count of a full top-K
 * table. While the table has free slots the minimum is reported as 0 so
 * that every key is admitted.
 */
static void
RefreshMinCount(ObsSketch *sketch)
{
	ObsTopKEntry *entries = SketchSlots(sketch);
	uint64_t minCount = UINT64_MAX;

	if (sketch->used < sketch->k)
	{
		__atomic_store_n(&sketch->min_count, 0, __ATOMIC_RELAXED);
		return;
	}
	for (uint32_t i = 0; i < sketch->k; i++)
	{
		if (entries[i].count < minCount)
		{
			minCount = entries[i].count;
		}
	}
	__atomic_store_n(&sketch->min_count, minCount, __ATOMIC_RELAXED);
}


/*
 * FillEntry overwrites a top-K slot with a new key.
 */
static void
FillEntry(ObsTopKEntry *entry, uint64_t hash, uint64_t count, uint64_t error,
		  uint64_t shardid, uint8_t access, const char *key)
{
	entry->hash = hash;
	entry->count = count;
	entry->error = error;
	entry->shardid = shardid;
	entry->access = access;
	if (key == NULL)
	{
		key = "";
	}
	strncpy(entry->key, key, OBS_SKETCH_KEY_LEN - 1);
	entry->key[OBS_SKETCH_KEY_LEN - 1] = '\0';
}


/*
 * obs_sketch_offer records a key with its current estimate in the top-K
 * table. A key already present has its count raised; a new key takes a
 * free slot or replaces the smallest entry if its estimate is larger, in
 * which case the replaced count is kept as the new entry's error. The
 * caller must hold an exclusive lock on the table. Returns 1 if the table
 * changed.
 */
int
obs_sketch_offer(ObsSketch *sketch, uint64_t hash, uint64_t estimate,
				 uint64_t shardid, uint8_t access, const char *key)
{
	ObsTopKEntry *entries = SketchSlots(sketch);
	int freeSlot = -1;
	int minSlot = -1;

	if (sketch->k == 0 || hash == 0)
	{
		return 0;
	}

	for (uint32_t i = 0; i < sketch->k; i++)
	{
		if (entries[i].hash == hash)
		{
			if (estimate <= entries[i].count)
			{
				return 0;
			}
			entries[i].count = estimate;
			RefreshMinCount(sketch);
			return 1;
		}

		if (entries[i].hash == 0)
		{
			if (freeSlot < 0)
			{
				freeSlot = (int) i;
			}
		}
		else if (minSlot < 0 || entries[i].count < entries[minSlot].count)
		{
			minSlot = (int) i;
		}
	}

	if (freeSlot >= 0)
	{
		FillEntry(&entries[freeSlot], hash, estimate, 0, shardid, access, key);
		sketch->used++;
		RefreshMinCount(sketch);
		return 1;
	}

	if (minSlot >= 0 && estimate > entries[minSlot].count)
	{
		uint64_t replacedCount = entries[minSlot].count;

		FillEntry(&entries[minSlot], hash, estimate, replacedCount, shardid, access, key);
		RefreshMinCount(sketch);
		return 1;
	}
	return 0;
}


/*
 * obs_sketch_topk copies up to max occupied top-K entries into out, sorted
 * by count descending, and returns how many were copied. The caller must
 * hold at least a shared lock on the table.
 */
int
obs_sketch_topk(const ObsSketch *sketch, ObsTopKEntry *out, int max)
{
	ObsTopKEntry *entries = SketchSlots(sketch);
	int count = 0;

	for (uint32_t i = 0; i < sketch->k && count < max; i++)
	{
		if (entries[i].hash != 0)
		{
			out[count++] = entries[i];
		}
	}

	/* insertion sort is fine, k is small */
	for (int i = 1; i < count; i++)
	{
		ObsTopKEntry current = out[i];
		int j = i - 1;

		while (j >= 0 && out[j].count < current.count)
		{
			out[j + 1] = out[j];
			j--;
		}
		out[j + 1] = current;
	}
	return count;
}


/*
 * obs_sketch_total returns the number of increments since the last reset.
 */
uint64_t
obs_sketch_total(const ObsSketch *sketch)
{
	return __atomic_load_n(&sketch->total, __ATOMIC_RELAXED);
}
