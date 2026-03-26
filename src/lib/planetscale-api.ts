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

export interface Actor {
  id: string;
  display_name: string;
  avatar_url: string;
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
  options: RequestInit = {},
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
        details,
      );
    }

    if (response.status === 401 || response.status === 403) {
      throw new PlanetScaleAPIError(
        "Permission denied. Please check your API token has the required permissions.",
        response.status,
        details,
      );
    }

    throw new PlanetScaleAPIError(
      `API request failed: ${response.statusText}`,
      response.status,
      details,
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
  authHeader: string,
): Promise<Database> {
  return apiRequest<Database>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}`,
    authHeader,
  );
}

/**
 * Get branch information including replica availability
 */
export async function getBranch(
  organization: string,
  database: string,
  branch: string,
  authHeader: string,
): Promise<Branch> {
  return apiRequest<Branch>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}`,
    authHeader,
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
  replica?: boolean,
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
    },
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
  authHeader: string,
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
    },
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
  options?: { successor?: string },
): Promise<void> {
  await apiRequest<void>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/roles/${encodeURIComponent(roleId)}`,
    authHeader,
    {
      method: "DELETE",
      body: options?.successor
        ? JSON.stringify({ successor: options.successor })
        : undefined,
    },
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
  authHeader: string,
): Promise<void> {
  await apiRequest<void>(
    `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/passwords/${encodeURIComponent(passwordId)}`,
    authHeader,
    { method: "DELETE" },
  );
}

// --- Traffic Control ---

export interface TrafficRuleTag {
  key_id: string;
  key: string;
  value: string;
  source: "sql" | "system";
}

export interface TrafficRule {
  id: string;
  kind: "match";
  tags: TrafficRuleTag[];
  fingerprint: string;
  keyspace: string;
  actor: Actor;
  syntax_highlighted_sql: string;
  created_at: string;
}

export type TrafficBudgetMode = "enforce" | "warn" | "off";

export interface TrafficBudget {
  id: string;
  name: string;
  mode: TrafficBudgetMode;
  capacity: number | null;
  rate: number | null;
  burst: number | null;
  concurrency: number | null;
  actor: Actor;
  rules: TrafficRule[];
  created_at: string;
  updated_at: string;
}

export interface PaginatedResponse<T> {
  current_page: number;
  next_page: number | null;
  next_page_url: string | null;
  prev_page: number | null;
  prev_page_url: string | null;
  data: T[];
}

function branchPath(
  organization: string,
  database: string,
  branch: string,
): string {
  return `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}`;
}

export async function listTrafficBudgets(
  organization: string,
  database: string,
  branch: string,
  authHeader: string,
  opts?: {
    page?: number;
    per_page?: number;
    period?: string;
    fingerprint?: string;
    created_at?: string;
  },
): Promise<PaginatedResponse<TrafficBudget>> {
  const params = new URLSearchParams();
  if (opts?.page) params.set("page", String(opts.page));
  if (opts?.per_page) params.set("per_page", String(opts.per_page));
  if (opts?.period) params.set("period", opts.period);
  if (opts?.fingerprint) params.set("fingerprint", opts.fingerprint);
  if (opts?.created_at) params.set("created_at", opts.created_at);
  const qs = params.toString();
  return apiRequest<PaginatedResponse<TrafficBudget>>(
    `${branchPath(organization, database, branch)}/traffic/budgets${qs ? `?${qs}` : ""}`,
    authHeader,
  );
}

export async function getTrafficBudget(
  organization: string,
  database: string,
  branch: string,
  id: string,
  authHeader: string,
): Promise<TrafficBudget> {
  return apiRequest<TrafficBudget>(
    `${branchPath(organization, database, branch)}/traffic/budgets/${encodeURIComponent(id)}`,
    authHeader,
  );
}

export interface CreateTrafficBudgetInput {
  name: string;
  mode: TrafficBudgetMode;
  capacity?: number;
  rate?: number;
  burst?: number;
  concurrency?: number;
  rules?: string[];
}

export async function createTrafficBudget(
  organization: string,
  database: string,
  branch: string,
  body: CreateTrafficBudgetInput,
  authHeader: string,
): Promise<TrafficBudget> {
  return apiRequest<TrafficBudget>(
    `${branchPath(organization, database, branch)}/traffic/budgets`,
    authHeader,
    { method: "POST", body: JSON.stringify(body) },
  );
}

export interface UpdateTrafficBudgetInput {
  name?: string;
  mode?: TrafficBudgetMode;
  capacity?: number;
  rate?: number;
  burst?: number;
  concurrency?: number;
  rules?: string[];
}

export async function updateTrafficBudget(
  organization: string,
  database: string,
  branch: string,
  id: string,
  body: UpdateTrafficBudgetInput,
  authHeader: string,
): Promise<TrafficBudget> {
  return apiRequest<TrafficBudget>(
    `${branchPath(organization, database, branch)}/traffic/budgets/${encodeURIComponent(id)}`,
    authHeader,
    { method: "PATCH", body: JSON.stringify(body) },
  );
}

export async function deleteTrafficBudget(
  organization: string,
  database: string,
  branch: string,
  id: string,
  authHeader: string,
): Promise<void> {
  await apiRequest<void>(
    `${branchPath(organization, database, branch)}/traffic/budgets/${encodeURIComponent(id)}`,
    authHeader,
    { method: "DELETE" },
  );
}

export interface CreateTrafficRuleInput {
  kind: "match";
  tags?: Array<{ key: string; value: string }>;
  fingerprint?: string;
}

export async function createTrafficRule(
  organization: string,
  database: string,
  branch: string,
  budgetId: string,
  body: CreateTrafficRuleInput,
  authHeader: string,
): Promise<TrafficRule> {
  return apiRequest<TrafficRule>(
    `${branchPath(organization, database, branch)}/traffic/budgets/${encodeURIComponent(budgetId)}/rules`,
    authHeader,
    { method: "POST", body: JSON.stringify(body) },
  );
}

export async function deleteTrafficRule(
  organization: string,
  database: string,
  branch: string,
  budgetId: string,
  ruleId: string,
  authHeader: string,
): Promise<void> {
  await apiRequest<void>(
    `${branchPath(organization, database, branch)}/traffic/budgets/${encodeURIComponent(budgetId)}/rules/${encodeURIComponent(ruleId)}`,
    authHeader,
    { method: "DELETE" },
  );
}
