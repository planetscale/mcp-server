import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError, USER_AGENT } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";
import {
  apiErrorMessage,
  errorMessage,
  EXTENDED_FROM_DESCRIPTION,
  EXTENDED_RANGE_NOTE,
  EXTENDED_TO_DESCRIPTION,
  INSIGHTS_PERIODS,
  resolveEnv,
  resultFields,
  type InsightsPeriod,
} from "../lib/insights-tools.ts";
import {
  SUMMARY_FIELD_KEYS,
  SUMMARY_FIELD_NAMES,
  summaryResponseKeys,
  type SummaryField,
} from "../lib/query-summary-fields.ts";

const API_BASE = "https://api.planetscale.com/v1";

const TABLET_TYPES = ["primary", "replica", "rdonly"] as const;

const STATEMENT_TYPES = ["SELECT", "INSERT", "UPDATE", "DELETE"] as const;

const DEFAULT_LIMIT = 25;
const MAX_LIMIT = 100;

const DEFAULT_VALUES_LIMIT = 25;
const MAX_VALUES_LIMIT = 100;

// The tag listing declares a `per_page` param but never reads it: the API
// returns at most this many tags, always. There is no limit for a caller to
// raise, so `truncated` here means "narrow the filters" rather than "ask for
// more".
const TAG_LIST_CAP = 100;

// Curated metrics requested by default. Omitting `fields` server-side returns
// every metric the API knows about, which is mostly zero-fill for any single
// question. `count` and `totalTime` also keep the always-serialized derived
// `time_per_query` meaningful.
const DEFAULT_SUMMARY_FIELDS = [
  "dimensions",
  "count",
  "errorCount",
  "totalTime",
  "percentTime",
  "p50Latency",
  "p99Latency",
  "rowsRead",
  "rowsReadPerReturned",
  "rowsAffected",
  "egressBytes",
  "lastRun",
] as const satisfies readonly SummaryField[];

type QueryParams = Record<string, string | string[] | undefined>;

interface ListResponse<T> {
  data: T[];
  dimension_counts?: { collapsed_count: number; total_count: number } | null;
}

/**
 * Bound a caller-supplied count to 1..max, matching the bounds the API applies
 * itself so an out-of-range request returns results instead of an error.
 */
export function clampLimit(value: number, max: number): number {
  return Math.min(Math.max(Math.trunc(value), 1), max);
}

/**
 * Drop absent values (null, empty arrays) and the `type` annotation the API
 * stamps on every object, for token efficiency. Applies recursively to objects nested
 * inside arrays (a tag's `values`).
 *
 * Recorded zeros are kept, because a metric the branch actually reported as 0
 * means something different from one that went unreported. The catch is that
 * the summaries endpoint returns every metric key it knows about on every row
 * and reports the ones we did not request as 0, so an unrequested 0 carries no
 * information while a requested 0 does. `requestedKeys` separates
 * the two -- zeros survive only for keys we asked the API to compute. A nonzero
 * value is always kept, asked for or not, since padding is never nonzero.
 *
 * Pass no `requestedKeys` for the endpoints that take no `fields` param (the
 * tag listing and single-tag lookup); there every zero is genuine.
 *
 * The zero rule is deliberately not carried into nested objects. `requestedKeys`
 * holds row-level metric keys, and an object inside an array (a tag's `values`,
 * a row's `table_keyspaces`) has its own key namespace that can never intersect
 * it -- so applying the rule there would drop every genuine nested zero, and
 * nothing inside those objects is zero-fill padding to begin with.
 */
export function compactEntry(
  entry: Record<string, unknown>,
  requestedKeys?: ReadonlySet<string>
): Record<string, unknown> {
  const filtered: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(entry)) {
    if (key === "type") continue;
    if (value === undefined || value === null) continue;
    if (value === 0 && requestedKeys && !requestedKeys.has(key)) continue;
    if (Array.isArray(value)) {
      if (value.length === 0) continue;
      filtered[key] = value.map((v) =>
        v !== null && typeof v === "object" && !Array.isArray(v)
          ? compactEntry(v as Record<string, unknown>)
          : v
      );
      continue;
    }
    filtered[key] = value;
  }
  return filtered;
}

/**
 * Restore a `values: []` that compaction dropped as an empty array.
 *
 * Absence would be ambiguous in a way the other stripped keys are not: a caller
 * cannot tell "this tag has no values in this window" from "this tool does not
 * return values". That gets actively misleading with `literal_values_only`,
 * where a tag whose values were all collapsed comes back with a nonzero
 * `query_count` and, without this, no `values` key at all.
 */
function withValues(tag: Record<string, unknown>): Record<string, unknown> {
  return "values" in tag ? tag : { ...tag, values: [] };
}

// Input fields shared by all three tag tools. Unlike the error tools, `limit`
// is not here: only the summaries endpoint honours `per_page`.
const branchInputSchema = {
  organization: z.string().describe("PlanetScale organization name"),
  database: z.string().describe("Database name"),
  branch: z.string().describe("Branch name (e.g., 'main')"),
  period: z
    .enum(INSIGHTS_PERIODS)
    .optional()
    .describe(
      "Shorthand for a recent time window ending at now. Cannot be combined with from/to."
    ),
  from: z.string().optional().describe(EXTENDED_FROM_DESCRIPTION),
  to: z.string().optional().describe(EXTENDED_TO_DESCRIPTION),
};

// Filters accepted by the tag listing and the single-tag lookup. The summaries
// endpoint reads none of these -- it filters through `query` instead.
const tagFilterInputSchema = {
  fingerprint: z
    .string()
    .optional()
    .describe(
      "Only include tags that appear on queries with this fingerprint. Get one from `get_insights`."
    ),
  keyspace: z
    .string()
    .optional()
    .describe(
      "Only include tags that appear on queries against this keyspace."
    ),
  tablet_type: z
    .enum(TABLET_TYPES)
    .optional()
    .describe("Filter by tablet type: 'primary', 'replica', or 'rdonly'."),
  values_limit: z
    .number()
    .optional()
    .describe(
      `Maximum number of literal values returned per tag (default: ${DEFAULT_VALUES_LIMIT}, max: ${MAX_VALUES_LIMIT}). Values outside that range are clamped. The 'Other' and 'Collapsed' buckets are counted separately and still returned.`
    ),
  literal_values_only: z
    .boolean()
    .optional()
    .describe(
      "Return only real recorded values, dropping the synthetic 'Other' (overflow) and 'Collapsed' entries. Note that 'Other' is the only signal that values beyond `values_limit` exist, so dropping it leaves no indication that the value list is partial -- the response's `truncated` field counts tags, not values."
    ),
};

interface BranchInput {
  organization: string;
  database: string;
  branch: string;
  period?: InsightsPeriod | undefined;
  from?: string | undefined;
  to?: string | undefined;
}

interface TagFilterInput {
  fingerprint?: string | undefined;
  keyspace?: string | undefined;
  tablet_type?: (typeof TABLET_TYPES)[number] | undefined;
  values_limit?: number | undefined;
  literal_values_only?: boolean | undefined;
}

interface RequestContext {
  authHeader: string;
  basePath: string;
  timeParams: QueryParams;
}

/**
 * Resolve auth, the branch tags endpoint path, and the shared time query
 * params. Returns an error string instead of throwing when the request can't be
 * built.
 */
function buildRequestContext(
  env: Record<string, string | undefined>,
  input: BranchInput
): RequestContext | string {
  if (!getAuthToken(env)) {
    return "Error: No PlanetScale authentication configured.";
  }

  const { organization, database, branch } = input;
  if (!organization || !database || !branch) {
    return "Error: organization, database, and branch are required";
  }

  if (input.period && (input.from || input.to)) {
    return "Error: 'period' cannot be combined with 'from'/'to'. Use either period (e.g. '1h', '6h') for a recent window, or from/to for a specific time range.";
  }

  return {
    authHeader: getAuthHeader(env),
    basePath: `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/insights/tags`,
    timeParams: {
      period: input.period,
      from: input.from,
      to: input.to,
    },
  };
}

/** The value filters shared by the listing and single-tag lookup. */
function tagFilterParams(input: TagFilterInput): QueryParams {
  const valuesLimit = input.values_limit;
  return {
    fingerprint: input.fingerprint,
    keyspace: input.keyspace,
    tablet_type: input.tablet_type,
    values_limit:
      valuesLimit === undefined
        ? undefined
        : clampLimit(valuesLimit, MAX_VALUES_LIMIT).toString(),
    literal_values_only: input.literal_values_only?.toString(),
  };
}

async function fetchTagsAPI<T>(
  endpoint: string,
  params: QueryParams,
  authHeader: string,
  signal: AbortSignal
): Promise<T> {
  const url = new URL(`${API_BASE}${endpoint}`);
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined) continue;
    // Repeated `key[]=` is required, not cosmetic: the API errors outright when
    // `fields` arrives as a single scalar instead of a list.
    if (Array.isArray(value)) {
      for (const v of value) url.searchParams.append(`${key}[]`, v);
    } else {
      url.searchParams.set(key, value);
    }
  }

  const response = await fetch(url, {
    method: "GET",
    signal,
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
      "User-Agent": USER_AGENT,
    },
  });

  if (!response.ok) {
    // Read the body once -- response.json() consumes it even when parsing
    // fails, so a text() fallback on the same response would throw and mask
    // the status-specific messages below.
    const raw = await response.text();
    let details: unknown;
    try {
      details = JSON.parse(raw);
    } catch {
      details = raw;
    }

    if (response.status === 404) {
      throw new PlanetScaleAPIError(
        "Query tags not found. Check your organization, database, and branch names, and that Insights is enabled for this database. Looking up a single tag also 404s when the id is unknown or is missing its 'S'/'B' prefix.",
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

    if (response.status === 400) {
      throw new PlanetScaleAPIError(
        apiErrorMessage(details) ??
          "Invalid request. Tag ids must keep the 'S' or 'B' prefix they were listed with by `list_query_tags`.",
        response.status,
        details
      );
    }

    throw new PlanetScaleAPIError(
      `Failed to fetch query tags: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as T;
}

export const queryTagsGram = new Gram()
  .tool({
    name: "list_query_tags",
    description:
      "List the query tags seen on a PlanetScale database branch's queries, with their values and query counts. Query tags are SQLCommenter-style annotations an application attaches in SQL comments (e.g. 'app', 'controller', 'route'), plus dimensions Insights derives from the connection itself (e.g. 'username', 'application_name'). Start here: the `id` values this returns are the only way to reach `get_query_tag` and `list_query_tag_summaries`, which is where query load actually gets attributed to a tag value. Each `id` keeps a one-character origin prefix -- 'S' for a tag the application submitted in a SQL comment (source: 'sql'), 'B' for one Insights captured itself (source: 'system') -- and `name` is the same id with the prefix stripped. Each value entry carries a `kind`: 'literal' is a real recorded value; 'overflow' is the synthetic 'Other' bucket aggregating values ranked beyond `values_limit`; 'collapsed' is the synthetic 'Collapsed' bucket counting queries where Insights had stopped recording this tag's values because it saw too many distinct ones, so those values are unrecoverable rather than zero. Data comes from PlanetScale Insights. " +
      EXTENDED_RANGE_NOTE,
    inputSchema: {
      ...branchInputSchema,
      ...tagFilterInputSchema,
      name_pattern: z
        .string()
        .optional()
        .describe(
          "Filter tags by a SQL LIKE pattern matched against the prefixed id. Wildcards are not added for you, so a bare word like 'app' matches nothing: use '%app%' to find 'Sapp', where '%' matches any run of characters and '_' matches exactly one (so '_app' matches both 'Sapp' and 'Bapp')."
        ),
    },
    async execute(ctx, input) {
      try {
        const request = buildRequestContext(resolveEnv(ctx.env), input);
        if (typeof request === "string") {
          return ctx.text(request);
        }

        const body = await fetchTagsAPI<ListResponse<Record<string, unknown>>>(
          request.basePath,
          {
            ...request.timeParams,
            ...tagFilterParams(input),
            q: input["name_pattern"],
          },
          request.authHeader,
          ctx.signal
        );

        const entries = body.data || [];

        return ctx.json({
          ...resultFields(entries.length, TAG_LIST_CAP),
          tags: entries.map((entry) => withValues(compactEntry(entry))),
        });
      } catch (error) {
        return ctx.text(errorMessage(error));
      }
    },
  })
  .tool({
    name: "get_query_tag",
    description:
      "Fetch one query tag on a PlanetScale database branch, with its values and their query counts. Pass the `id` from `list_query_tags`, prefix included -- an unprefixed or unknown id returns a not-found error. Value entries carry the same `kind` ('literal', 'overflow', 'collapsed') as the listing. Use this to re-read a single tag's values under different filters -- a narrower time window, a specific fingerprint or keyspace, or `literal_values_only` -- without listing every tag on the branch again. Data comes from PlanetScale Insights. " +
      EXTENDED_RANGE_NOTE,
    inputSchema: {
      ...branchInputSchema,
      ...tagFilterInputSchema,
      tag: z
        .string()
        .describe(
          "The tag id to fetch, e.g. 'Sapp' or 'Busername'. Use an `id` value from `list_query_tags`, keeping its 'S'/'B' prefix. A tag whose name contains a '.' cannot be fetched here, because the API's route splits the path on it."
        ),
    },
    async execute(ctx, input) {
      try {
        const request = buildRequestContext(resolveEnv(ctx.env), input);
        if (typeof request === "string") {
          return ctx.text(request);
        }

        const tag = input["tag"];
        if (!tag) {
          return ctx.text("Error: tag is required");
        }

        // Unlike the two list endpoints, this one returns the tag as the
        // top-level object with no list envelope.
        const entry = await fetchTagsAPI<Record<string, unknown>>(
          `${request.basePath}/${encodeURIComponent(tag)}`,
          {
            ...request.timeParams,
            ...tagFilterParams(input),
          },
          request.authHeader,
          ctx.signal
        );

        return ctx.json({ tag: withValues(compactEntry(entry)) });
      } catch (error) {
        return ctx.text(errorMessage(error));
      }
    },
  })
  .tool({
    name: "list_query_tag_summaries",
    description:
      "Break query statistics on a PlanetScale database branch down by query tag value -- total time, latency, rows read, and errors per application, route, job, or user. This is the tool that answers 'which part of my app is causing this load', as opposed to `get_insights`, which answers 'which query'. Pass one or more tag `id` values from `list_query_tags` in `tags`; several ids group by the combination of their values. Each row's `dimensions` maps the grouped tag ids to that row's values. Rows are sorted descending by `sort_by` (default 'totalTime'). A curated set of metrics is requested by default; `fields` widens or narrows it, and the response echoes the effective set as a map from each requested name to the key it is serialized under, since the two differ (`count` arrives as `query_count`). Metrics outside that set are stripped, so a 0 on a returned metric is a real measurement rather than an unrequested field. The `sort_by` metric is always included, whether or not it was asked for. `dimension_counts` reports how many matched queries had the grouping tag's value collapsed (`collapsed_count`) out of the total (`total_count`) -- those queries carry the tag, but their value was never recorded and cannot be recovered. Data comes from PlanetScale Insights. " +
      EXTENDED_RANGE_NOTE,
    inputSchema: {
      ...branchInputSchema,
      tags: z
        .array(z.string())
        .min(1)
        .describe(
          "Tag ids to group by, taken from the `id` values returned by `list_query_tags` with their prefix intact, e.g. ['Sapp'] or ['Sapp', 'Broute']. Several ids group by the combination of their values. An id missing its 'S'/'B' prefix is rejected."
        ),
      query: z
        .string()
        .optional()
        .describe(
          "Filter the query statistics being summarized. This is a structured search, not a plain substring: supported terms include `fingerprint:<hash>`, `keyspace:<name>`, `tag:<key>:<value>` (or `tag:<key>` for any value), `user:<name>`, `statement_type:<type>`, `table:<name>`, `index:<name>`, and comparisons like `p99:>100`, while a bare word matches the normalized SQL. Prefix a term with '!' to negate it. This is also the only way to restrict these summaries to one fingerprint or keyspace -- unlike `list_query_tags`, this endpoint has no separate fingerprint or keyspace filter."
        ),
      sort_by: z
        .enum(SUMMARY_FIELD_NAMES)
        .optional()
        .describe(
          "Metric to sort rows by, descending. Defaults to 'totalTime'. Accepts any name listed under `fields`."
        ),
      statement_type: z
        .enum(STATEMENT_TYPES)
        .optional()
        .describe("Only summarize queries of this statement type."),
      fields: z
        .array(z.enum(SUMMARY_FIELD_NAMES))
        .min(1)
        .optional()
        .describe(
          `Metric fields to return. Defaults to a curated set (${DEFAULT_SUMMARY_FIELDS.join(", ")}), because omitting this server-side returns all ${SUMMARY_FIELD_NAMES.length} metrics and most of them will be zero-fill for any one question. 'dimensions' is always included, since without it the rows cannot be told apart. The response echoes the effective set as \`fields\`, mapping each requested name to the key it is serialized under (\`count\` arrives as \`query_count\`); metrics outside that set are stripped, so a 0 on a returned metric is a real measurement.`
        ),
      tablet_type: z
        .enum(TABLET_TYPES)
        .optional()
        .describe("Filter by tablet type: 'primary', 'replica', or 'rdonly'."),
      limit: z
        .number()
        .optional()
        .describe(
          `Maximum number of summary rows to return (default: ${DEFAULT_LIMIT}, max: ${MAX_LIMIT}). This endpoint does not paginate, so when a response reports truncated: true, raise this limit or narrow the time window, tags, or query.`
        ),
    },
    async execute(ctx, input) {
      try {
        const request = buildRequestContext(resolveEnv(ctx.env), input);
        if (typeof request === "string") {
          return ctx.text(request);
        }

        const groupBy = input["tags"];
        if (!groupBy || groupBy.length === 0) {
          return ctx.text("Error: tags must name at least one tag id");
        }

        const sortBy = input["sort_by"] ?? "totalTime";

        const requested = input["fields"] ?? [...DEFAULT_SUMMARY_FIELDS];
        const summaryFields: SummaryField[] = requested.includes("dimensions")
          ? [...requested]
          : ["dimensions", ...requested];
        // The API computes the sort column whether or not it was requested, so
        // leaving it out of `fields` does not remove it from the rows -- it
        // removes it only from the rows where it measured zero, since those are
        // indistinguishable from zero-fill. Ordering by a metric that appears
        // on some rows and not others reads as broken output, so ask for it.
        if (!summaryFields.includes(sortBy)) {
          summaryFields.push(sortBy);
        }
        const limit = clampLimit(input["limit"] ?? DEFAULT_LIMIT, MAX_LIMIT);

        const body = await fetchTagsAPI<ListResponse<Record<string, unknown>>>(
          `${request.basePath}/summaries`,
          {
            ...request.timeParams,
            tags: groupBy,
            q: input["query"],
            sort: sortBy,
            // The API defaults to ascending, which would put the cheapest
            // queries first -- the opposite of what any caller wants here.
            dir: "desc",
            tablet_type: input["tablet_type"],
            type: input["statement_type"],
            fields: summaryFields,
            per_page: limit.toString(),
          },
          request.authHeader,
          ctx.signal
        );

        const entries = body.data || [];
        const requestedKeys = summaryResponseKeys(summaryFields);

        return ctx.json({
          grouped_by: groupBy,
          sort_by: sortBy,
          // Names the metrics actually requested, so a 0 in a row reads as a
          // real measurement rather than a field we never asked for. Paired
          // with the key each one is serialized under, because the two
          // vocabularies differ -- asking for `count` and reading `query_count`
          // is otherwise an unstated mapping the caller cannot apply.
          fields: Object.fromEntries(
            summaryFields.map((field) => [field, SUMMARY_FIELD_KEYS[field]])
          ),
          ...resultFields(entries.length, limit),
          ...(body.dimension_counts
            ? { dimension_counts: body.dimension_counts }
            : {}),
          summaries: entries.map((entry) => compactEntry(entry, requestedKeys)),
        });
      } catch (error) {
        return ctx.text(errorMessage(error));
      }
    },
  });
