/**
 * Split a time range into N equal-duration shards
 * 
 * Usage:
 *   const shards = shardRange("2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z", 4);
 *   // Returns array of { shard, min, max } objects
 */
export function shardRange(minTime, maxTime, numShards) {
  const start = new Date(minTime);
  const end = new Date(maxTime);

  const totalMs = end.getTime() - start.getTime();
  const shardMs = totalMs / numShards;

  const shards = [];

  for (let i = 0; i < numShards; i++) {
    const s = new Date(start.getTime() + shardMs * i);
    const e = i === numShards - 1
      ? end
      : new Date(start.getTime() + shardMs * (i + 1));

    shards.push({
      shard: i,
      min: s.toISOString(),
      max: e.toISOString(),
    });
  }

  return shards;
}
