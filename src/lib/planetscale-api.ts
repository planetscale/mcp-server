const API_BASE = "https://api.planetscale.com/v1";

export type DatabaseKind = "mysql" | "postgresql";

export interface Database {
  id: string;
  name: string;
  kind: DatabaseKind;
  default_branch: string;
  state: string;
}

export interface Branch {
  id: string;
  name: string;
  kind: DatabaseKind;
  production: boolean;
  has_replicas: boolean;
}

export interface VitessCredentials {
  id: string;
  username: string;
  password: string;
  host: string;
  database_branch: {
    name: string;
    id: string;
  };
}

export interface PostgresCredentials {
  id: string;
  username: string;
  password: string;
  host: string;
  database_name: string;
  branch: {
    name: string;
    id: string;
  };
  replica?: boolean;
}

export type VitessRole = "reader" | "writer" | "admin" | "readwriter";

export type PostgresInheritedRole =
  | "pscale_managed"
  | "pg_checkpoint"
  | "pg_create_subscription"
  | "pg_maintain"
  | "pg_monitor"
  | "pg_read_all_data"
  | "pg_read_all_settings"
  | "pg_read_all_stats"
  | "pg_signal_backend"
  | "pg_stat_scan_tables"
  | "pg_use_reserved_connections"
  | "pg_write_all_data"
  | "postgres";

export class PlanetScaleAPIError extends Error {
  statusCode: number;
  details?: unknown;

  constructor(message: string, statusCode: number, details?: unknown) {
    super(message);
    this.name = "PlanetScaleAPIError";
    this.statusCode = statusCode;
    this.details = details;
  }
}

async function apiRequest<T>(
  endpoint: string,
  authHeader: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;

  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: authHeader,
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  if (!response.ok) {
    let details: unknown;
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }

    if (response.status === 404) {
      throw new PlanetScaleAPIError(
        "Resource not found. Please check your organization, database, and branch names.",
        response.status,
        details
      );
    }

    if (response.status === 401 || response.status === 403) {
      throw new PlanetScaleAPIError(
        "Permission denied. Please check your API token has the required permissions.",
        response.status,
        details
      );
    }

    throw new PlanetScaleAPIError(
      `API request failed: ${response.statusText}`,
      response.status,
      details
    );
  }

  // Handle 204 No Content responses (e.g., DELETE requests)
  if (response.status === 204) {
    return undefined as T;
  }

  return response.json() as Promise<T>;
}

/**
 * Get database information including its type (mysql/vitess or postgresql)
 */
export async function getDatabase(
  organization: string,
  database: string,
  authHeader: string
): Promise<Database> {
  return apiRequest<Database>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}`,
    authHeader
  );
}

/**
 * Get branch information including replica availability
 */
export async function getBranch(
  organization: string,
  database: string,
  branch: string,
  authHeader: string
): Promise<Branch> {
  return apiRequest<Branch>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}`,
    authHeader
  );
}

/**
 * Create short-lived credentials for a Vitess (MySQL) database
 */
export async function createVitessCredentials(
  organization: string,
  database: string,
  branch: string,
  role: VitessRole,
  authHeader: string,
  replica?: boolean
): Promise<VitessCredentials> {
  const timestamp = Date.now();
  const name = `mcp-query-${timestamp}`;

  const response = await apiRequest<{
    id: string;
    username: string;
    plain_text: string;
    access_host_url: string;
    database_branch: {
      name: string;
      id: string;
    };
  }>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/passwords`,
    authHeader,
    {
      method: "POST",
      body: JSON.stringify({
        name,
        role,
        ttl: 60, // 60 seconds TTL
        replica: replica ?? false,
      }),
    }
  );

  return {
    id: response.id,
    username: response.username,
    password: response.plain_text,
    host: response.access_host_url,
    database_branch: response.database_branch,
  };
}

/**
 * Create short-lived credentials for a Postgres database
 */
export async function createPostgresCredentials(
  organization: string,
  database: string,
  branch: string,
  inheritedRoles: PostgresInheritedRole[],
  authHeader: string
): Promise<PostgresCredentials> {
  const timestamp = Date.now();
  const name = `mcp-query-${timestamp}`;

  const response = await apiRequest<{
    id: string;
    username: string;
    password: string;
    access_host_url: string;
    database_name: string;
    branch: {
      name: string;
      id: string;
    };
  }>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/roles`,
    authHeader,
    {
      method: "POST",
      body: JSON.stringify({
        name,
        inherited_roles: inheritedRoles,
        ttl: 60, // 60 seconds TTL
      }),
    }
  );

  return {
    id: response.id,
    username: response.username,
    password: response.password,
    host: response.access_host_url,
    database_name: response.database_name,
    branch: response.branch,
  };
}

/**
 * Delete Postgres role credentials, optionally transferring ownership of
 * any objects created by this role to a successor role.
 *
 * @see https://planetscale.com/docs/api/reference/delete_role
 */
export async function deletePostgresRole(
  organization: string,
  database: string,
  branch: string,
  roleId: string,
  authHeader: string,
  options?: { successor?: string }
): Promise<void> {
  await apiRequest<void>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/roles/${encodeURIComponent(roleId)}`,
    authHeader,
    {
      method: "DELETE",
      body: options?.successor ? JSON.stringify({ successor: options.successor }) : undefined,
    }
  );
}

/**
 * Delete Vitess password credentials.
 */
export async function deleteVitessPassword(
  organization: string,
  database: string,
  branch: string,
  passwordId: string,
  authHeader: string
): Promise<void> {
  await apiRequest<void>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/passwords/${encodeURIComponent(passwordId)}`,
    authHeader,
    { method: "DELETE" }
  );
}

export interface TimeSeries {
  type?: string;
  metric: string;
  label?: string;
  labels?: Record<string, string>;
  points: number[][];
}

export interface MetricSeries {
  type?: string;
  start_date: string;
  end_date: string;
  interval: number;
  series: TimeSeries[];
}

export interface InstantMetric {
  metric: string;
  label?: string;
  values: Array<Record<string, unknown>>;
}

export interface InstantMetrics {
  type?: string;
  branch?: Record<string, unknown>;
  metrics: InstantMetric[];
}

export interface GetMetricSeriesRequest {
  organization: string;
  database: string;
  branch: string;
  metrics: string[];
  period?: string;
  from?: string;
  to?: string;
  steps?: number;
  tablet_type?: string;
  keyspace?: string;
  shard?: string;
  role?: string;
  container?: string;
  pod?: string;
  pods?: string[];
  query_ids?: string[];
  fingerprint?: string;
  budget_id?: string;
  rule_id?: string;
  q?: string;
}

export interface GetInstantMetricsRequest {
  organization: string;
  database: string;
  branch: string;
  metrics: string[];
  role?: string;
  shard?: string;
  container?: string;
  pod?: string;
}

function metricsAPIPath(
  organization: string,
  database: string,
  branch: string
): string {
  return `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/metrics`;
}

function setQueryValue(
  params: URLSearchParams,
  key: string,
  value?: string | number
): void {
  if (value === undefined || value === "") {
    return;
  }
  params.set(key, String(value));
}

function addQueryValues(
  params: URLSearchParams,
  key: string,
  values?: string[]
): void {
  if (!values) {
    return;
  }
  for (const value of values) {
    if (value) {
      params.append(key, value);
    }
  }
}

export function metricSeriesQuery(
  request: Omit<GetMetricSeriesRequest, "organization" | "database" | "branch">
): string {
  const params = new URLSearchParams();
  addQueryValues(params, "metrics[]", request.metrics);
  setQueryValue(params, "period", request.period);
  setQueryValue(params, "from", request.from);
  setQueryValue(params, "to", request.to);
  if (request.steps !== undefined && request.steps > 0) {
    params.set("steps", String(request.steps));
  }
  setQueryValue(params, "tablet_type", request.tablet_type);
  setQueryValue(params, "keyspace", request.keyspace);
  setQueryValue(params, "shard", request.shard);
  setQueryValue(params, "role", request.role);
  setQueryValue(params, "container", request.container);
  setQueryValue(params, "pod", request.pod);
  addQueryValues(params, "pods[]", request.pods);
  addQueryValues(params, "query_ids[]", request.query_ids);
  setQueryValue(params, "fingerprint", request.fingerprint);
  setQueryValue(params, "budget_id", request.budget_id);
  setQueryValue(params, "rule_id", request.rule_id);
  setQueryValue(params, "q", request.q);
  const query = params.toString();
  return query ? `?${query}` : "";
}

function instantMetricsQuery(
  request: Omit<GetInstantMetricsRequest, "organization" | "database" | "branch">
): string {
  const params = new URLSearchParams();
  addQueryValues(params, "metrics[]", request.metrics);
  setQueryValue(params, "role", request.role);
  setQueryValue(params, "shard", request.shard);
  setQueryValue(params, "container", request.container);
  setQueryValue(params, "pod", request.pod);
  const query = params.toString();
  return query ? `?${query}` : "";
}

export async function getMetricSeries(
  request: GetMetricSeriesRequest,
  authHeader: string
): Promise<MetricSeries> {
  return apiRequest<MetricSeries>(
    `${metricsAPIPath(request.organization, request.database, request.branch)}${metricSeriesQuery(request)}`,
    authHeader
  );
}

export async function getInstantMetrics(
  request: GetInstantMetricsRequest,
  authHeader: string
): Promise<InstantMetrics> {
  return apiRequest<InstantMetrics>(
    `${metricsAPIPath(request.organization, request.database, request.branch)}/instant${instantMetricsQuery(request)}`,
    authHeader
  );
}
