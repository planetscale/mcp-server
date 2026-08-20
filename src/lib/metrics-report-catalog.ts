export type ReportSectionKind = "series" | "instant";

export interface ReportSectionDefinition {
  name: string;
  kind: ReportSectionKind;
  metrics: string[];
}

const mysqlReportSections: ReportSectionDefinition[] = [
  {
    name: "Workload, errors, and traffic control",
    kind: "series",
    metrics: [
      "queries",
      "query_errors",
      "connections",
      "rows_read",
      "rows_returned",
      "rows_written",
      "violations",
      "traffic_control_warnings",
      "traffic_control_throttled",
    ],
  },
  {
    name: "Latency and execution time",
    kind: "series",
    metrics: [
      "latency_p50",
      "latency_p95",
      "latency_p99",
      "latency_p999",
      "latency_max",
      "vtgate_latency_p50",
      "vtgate_latency_p95",
      "total_duration_millis",
      "cpu_duration_millis",
      "io_duration_millis",
    ],
  },
  {
    name: "Query efficiency and fan-out",
    kind: "series",
    metrics: [
      "rows_read_per_query",
      "rows_returned_per_query",
      "rows_affected_per_query",
      "rows_read_per_returned",
      "avg_shard_queries",
      "max_shard_queries",
      "avg_parallel_workers",
    ],
  },
  {
    name: "Buffer and block activity",
    kind: "series",
    metrics: [
      "blocks_hit",
      "blocks_read",
      "block_cache_hit_ratio",
      "blocks_dirtied",
      "blocks_written",
    ],
  },
  {
    name: "Network traffic",
    kind: "series",
    metrics: [
      "ingress_bytes",
      "ingress_bytes_per_query",
      "max_ingress_bytes",
      "egress_bytes",
      "egress_bytes_per_query",
      "max_egress_bytes",
    ],
  },
  {
    name: "VTGate utilization by availability zone",
    kind: "series",
    metrics: [
      "vtgate_requests",
      "vtgate_cpu_by_az",
      "vtgate_cpu_avg_by_az",
      "vtgate_memory_by_az",
      "vtgate_memory_avg_by_az",
    ],
  },
  {
    name: "Storage by table",
    kind: "series",
    metrics: ["storage_per_table"],
  },
];

const postgresReportSections: ReportSectionDefinition[] = [
  {
    name: "Workload, errors, and traffic control",
    kind: "series",
    metrics: [
      "queries",
      "query_errors",
      "connections",
      "rows_read",
      "rows_returned",
      "rows_written",
      "violations",
      "traffic_control_warnings",
      "traffic_control_throttled",
    ],
  },
  {
    name: "Latency and execution time",
    kind: "series",
    metrics: [
      "latency_p50",
      "latency_p95",
      "latency_p99",
      "latency_p999",
      "latency_max",
      "total_duration_millis",
      "cpu_duration_millis",
      "io_duration_millis",
    ],
  },
  {
    name: "Query efficiency and distribution",
    kind: "series",
    metrics: [
      "rows_read_per_query",
      "rows_returned_per_query",
      "rows_affected_per_query",
      "rows_read_per_returned",
      "avg_shard_queries",
      "max_shard_queries",
      "avg_parallel_workers",
    ],
  },
  {
    name: "Buffer and block activity",
    kind: "series",
    metrics: [
      "blocks_hit",
      "blocks_read",
      "block_cache_hit_ratio",
      "blocks_dirtied",
      "blocks_written",
    ],
  },
  {
    name: "Query network traffic",
    kind: "series",
    metrics: [
      "ingress_bytes",
      "ingress_bytes_per_query",
      "max_ingress_bytes",
      "egress_bytes",
      "egress_bytes_per_query",
      "max_egress_bytes",
    ],
  },
  {
    name: "Edge network traffic",
    kind: "series",
    metrics: [
      "planetscale_edge_bytes_received",
      "planetscale_edge_bytes_received_rate",
      "planetscale_edge_bytes_sent",
      "planetscale_edge_bytes_sent_rate",
    ],
  },
  {
    name: "Connections and connection pooling",
    kind: "series",
    metrics: [
      "planetscale_dedicated_pgbouncer_current_connections",
      "planetscale_dedicated_pgbouncer_cpu_usage",
      "planetscale_dedicated_pgbouncer_memory_usage",
      "planetscale_pgbouncer_current_connections",
      "planetscale_pgbouncer_pools_client",
      "planetscale_pgbouncer_pools_server",
      "planetscale_primary_postgres_connection_state",
      "planetscale_replica_postgres_connection_state",
      "planetscale_primary_pgbouncer_cpu_util_percentages",
      "planetscale_primary_pgbouncer_mem_util_percentages",
      "planetscale_replica_pgbouncer_current_connections",
      "planetscale_replica_pgbouncer_cpu_util_percentages",
      "planetscale_replica_pgbouncer_mem_util_percentages",
    ],
  },
  {
    name: "CPU, memory utilization, and IOPS",
    kind: "series",
    metrics: [
      "planetscale_pods_cpu_util_percentages",
      "planetscale_pods_mem_util_percentages",
      "planetscale_pods_iops_total",
      "planetscale_primary_pods_cpu_util_percentages",
      "planetscale_primary_pods_mem_util_percentages",
      "planetscale_primary_pods_iops_total",
      "planetscale_replica_pods_cpu_util_percentages",
      "planetscale_replica_pods_mem_util_percentages",
      "planetscale_replica_pods_iops_total",
    ],
  },
  {
    name: "PostgreSQL memory composition",
    kind: "series",
    metrics: [
      "planetscale_primary_memory_rss_bytes",
      "planetscale_primary_memory_mmap_bytes",
      "planetscale_primary_memory_active_cache_bytes",
      "planetscale_primary_memory_inactive_cache_bytes",
      "planetscale_replica_memory_rss_bytes",
      "planetscale_replica_memory_mmap_bytes",
      "planetscale_replica_memory_active_cache_bytes",
      "planetscale_replica_memory_inactive_cache_bytes",
    ],
  },
  {
    name: "Storage utilization",
    kind: "series",
    metrics: [
      "planetscale_primary_storage_usage",
      "planetscale_replica_storage_usage_bytes",
      "planetscale_storage_usage_bytes",
      "planetscale_replica_volume_usage_percentages",
      "planetscale_volume_usage_percentages",
    ],
  },
  {
    name: "Transactions, replication, and WAL",
    kind: "series",
    metrics: [
      "planetscale_primary_xact_commit_rate",
      "planetscale_replica_lag_seconds",
      "planetscale_replication_slot_max_wal_retained_bytes",
      "planetscale_replication_slots_lost",
      "planetscale_settings_max_slot_wal_keep_size_bytes",
      "planetscale_wal_archiver_succeeded_rate",
      "planetscale_wal_archiver_failed_rate",
      "planetscale_wal_archiver_last_age_succeeded",
      "planetscale_wal_size_bytes",
    ],
  },
  {
    name: "Pod health",
    kind: "series",
    metrics: ["planetscale_pods_container_ooms"],
  },
  {
    name: "Current connection capacity",
    kind: "instant",
    metrics: [
      "planetscale_dedicated_pgbouncer_current_connections",
      "planetscale_dedicated_pgbouncer_current_client_connections",
      "planetscale_dedicated_pgbouncer_current_server_connections",
      "planetscale_dedicated_pgbouncer_max_connections",
      "planetscale_dedicated_pgbouncer_cpu_usage",
      "planetscale_dedicated_pgbouncer_memory_usage",
      "planetscale_pgbouncer_current_client_connections",
      "planetscale_pgbouncer_current_server_connections",
      "planetscale_pgbouncer_settings_max_client_conn",
      "planetscale_postgres_connection_state",
      "planetscale_postgres_settings_max_connections",
    ],
  },
  {
    name: "Current storage capacity",
    kind: "instant",
    metrics: [
      "planetscale_volume_disk_usage_bytes",
      "planetscale_volume_usage_percentage",
      "planetscale_volume_capacity_bytes",
    ],
  },
  {
    name: "Backup activity",
    kind: "instant",
    metrics: [
      "planetscale_backup_restore_active",
      "planetscale_backup_fetch_percent",
    ],
  },
];

export function reportSectionsForEngine(
  engine: string
): ReportSectionDefinition[] {
  if (engine === "mysql") {
    return mysqlReportSections;
  }
  if (engine === "postgresql") {
    return postgresReportSections;
  }
  throw new Error(
    `database engine "${engine}" is not supported by metrics report`
  );
}
