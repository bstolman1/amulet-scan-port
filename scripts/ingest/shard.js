import { DateTime } from "luxon";

/**
 * Split a time range into N equal-duration shards
 * 
 * Usage:
 *   const shards = shardRange("2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z", 4);
 *   // Returns array of { shard, min, max } objects
 */
export function shardRange(minTime, maxTime, numShards) {
  const start = DateTime.fromISO(minTime);
  const end = DateTime.fromISO(maxTime);

  const totalMs = end.toMillis() - start.toMillis();
  const shardMs = totalMs / numShards;

  const shards = [];

  for (let i = 0; i < numShards; i++) {
    const s = start.plus({ milliseconds: shardMs * i });
    const e = i === numShards - 1
      ? end
      : start.plus({ milliseconds: shardMs * (i + 1) });

    shards.push({
      shard: i,
      min: s.toISO(),
      max: e.toISO(),
    });
  }

  return shards;
}
