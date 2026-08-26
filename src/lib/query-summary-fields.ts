/**
 * The query-summary field vocabulary for the Insights endpoints that take a
 * `fields[]` parameter.
 *
 * Two names exist for every metric and no transform relates them: requests send
 * a camelCase api_name, responses come back under a snake_case key. `count`
 * goes out and `query_count` comes back; `totalTime` becomes
 * `sum_total_duration_millis`.
 *
 * Keeping the pair together matters because the API always serializes every
 * metric key it knows about and defaults the ones we did not request to 0
 * instead of omitting them, so the pairing is what distinguishes a measured 0
 * from padding.
 *
 * There is no endpoint that lists these -- unlike branch metrics, which have
 * /metrics/catalog -- so the vocabulary can only live here, maintained by hand
 * against what the endpoint accepts and returns. A name the API adds later is
 * rejected until it is added below, which is the price of catching a typo that
 * would otherwise be silently ignored.
 *
 * One accepted name is deliberately absent: `trafficControlBudgetsUsed`. The
 * API takes it in `fields[]` and `sort` but never returns a matching key, so
 * requesting it buys query work and no data.
 */
export const SUMMARY_FIELD_KEYS = {
  dimensions: "dimensions",
  lastRun: "last_run_at",
  count: "query_count",
  errorCount: "error_count",
  rowsRead: "sum_rows_read",
  rowsAffected: "sum_rows_affected",
  rowsReturned: "sum_rows_returned",
  rowsReadPerReturned: "rows_read_per_returned",
  rowsReadPerQuery: "rows_read_per_query",
  rowsReturnedPerQuery: "rows_returned_per_query",
  rowsAffectedPerQuery: "rows_affected_per_query",
  totalTime: "sum_total_duration_millis",
  cpuTime: "sum_cpu_duration_millis",
  ioTime: "sum_io_duration_millis",
  percentTime: "sum_total_duration_percent",
  percentCpuTime: "sum_cpu_duration_percent",
  percentIoTime: "sum_io_duration_percent",
  sumShardQueries: "sum_shard_queries",
  maxShardQueries: "max_shard_queries",
  avgShardQueries: "avg_shard_queries",
  avgParallelWorkers: "avg_parallel_workers",
  table: "tables",
  qualifiedTable: "qualified_tables",
  tableKeyspace: "table_keyspaces",
  indexes: "index_usages",
  routingIndexes: "routing_index_usages",
  p50Latency: "p50_latency",
  p99Latency: "p99_latency",
  maxLatency: "max_latency",
  egressBytes: "egress_bytes",
  egressBytesPerQuery: "egress_bytes_per_query",
  maxEgressBytes: "max_egress_bytes",
  ingressBytes: "ingress_bytes",
  ingressBytesPerQuery: "ingress_bytes_per_query",
  maxIngressBytes: "max_ingress_bytes",
  blocksRead: "blocks_read",
  blocksHit: "blocks_hit",
  blockCacheHitRatio: "block_cache_hit_ratio",
  blocksDirtied: "blocks_dirtied",
  blocksWritten: "blocks_written",
  trafficControlWarnings: "traffic_control_warnings",
  trafficControlThrottled: "traffic_control_throttled",
  trafficControlChecked: "traffic_control_checked",
} as const;

/** A name accepted in `fields[]` and `sort`. */
export type SummaryField = keyof typeof SUMMARY_FIELD_KEYS;

/**
 * Every accepted field name, as a non-empty tuple so it can feed `z.enum()`
 * directly. The model then gets the vocabulary from the same place the code
 * does rather than from a hand-maintained list in prose.
 */
export const SUMMARY_FIELD_NAMES = Object.keys(SUMMARY_FIELD_KEYS) as [
  SummaryField,
  ...SummaryField[],
];

/**
 * The response keys the given `fields` will be serialized under -- the set
 * whose zeros are real measurements rather than zero-fill padding.
 *
 * Callers reach this only through a `z.enum(SUMMARY_FIELD_NAMES)` input, so
 * every name maps and there is no unrecognized-name case to handle.
 */
export function summaryResponseKeys(
  fields: readonly SummaryField[]
): Set<string> {
  return new Set(fields.map((field) => SUMMARY_FIELD_KEYS[field]));
}
