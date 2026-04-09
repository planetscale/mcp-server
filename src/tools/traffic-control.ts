import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import {
  PlanetScaleAPIError,
  listTrafficBudgets,
  getTrafficBudget,
  createTrafficBudget,
  updateTrafficBudget,
  deleteTrafficBudget,
  createTrafficRule,
  deleteTrafficRule,
} from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

function getEnv(ctx: {
  env: Record<string, unknown>;
}): Record<string, string | undefined> {
  return Object.keys(ctx.env).length > 0
    ? (ctx.env as Record<string, string | undefined>)
    : process.env;
}

function handleError(
  ctx: { text: (s: string) => Response },
  error: unknown,
): Response {
  if (error instanceof PlanetScaleAPIError) {
    return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
  }
  if (error instanceof Error) {
    return ctx.text(`Error: ${error.message}`);
  }
  return ctx.text("Error: An unexpected error occurred");
}

const orgParam = z.string().describe("PlanetScale organization name");
const dbParam = z.string().describe("Database name");
const branchParam = z.string().describe("Branch name (e.g., 'main')");

export const trafficControlGram = new Gram()
  .tool({
    name: "list_traffic_budgets",
    description:
      "List traffic control budgets for a PlanetScale database branch. " +
      "Traffic budgets let you set rate limits and concurrency caps on query traffic. " +
      "Returns a paginated list of budgets with their rules.",
    inputSchema: {
      organization: orgParam,
      database: dbParam,
      branch: branchParam,
      page: z.number().optional().describe("Page number (default: 1)"),
      per_page: z
        .number()
        .optional()
        .describe("Results per page (default: 25)"),
      period: z
        .string()
        .optional()
        .describe("Time period filter (e.g., '1h', '24h', '7d')"),
      fingerprint: z
        .string()
        .optional()
        .describe("Filter budgets by query fingerprint"),
    },
    async execute(ctx, input) {
      try {
        const env = getEnv(ctx);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }
        const authHeader = getAuthHeader(env);
        const result = await listTrafficBudgets(
          input.organization,
          input.database,
          input.branch,
          authHeader,
          {
            page: input.page,
            per_page: input.per_page,
            period: input.period,
            fingerprint: input.fingerprint,
          },
        );
        return ctx.json(result);
      } catch (error) {
        return handleError(ctx, error);
      }
    },
  })
  .tool({
    name: "get_traffic_budget",
    description:
      "Get a specific traffic control budget by ID, including its rules.",
    inputSchema: {
      organization: orgParam,
      database: dbParam,
      branch: branchParam,
      id: z.string().describe("The ID of the traffic budget"),
    },
    async execute(ctx, input) {
      try {
        const env = getEnv(ctx);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }
        const authHeader = getAuthHeader(env);
        const result = await getTrafficBudget(
          input.organization,
          input.database,
          input.branch,
          input.id,
          authHeader,
        );
        return ctx.json(result);
      } catch (error) {
        return handleError(ctx, error);
      }
    },
  })
  .tool({
    name: "create_traffic_budget",
    description:
      "Create a traffic control budget on a branch. A budget defines rate limits for matching query traffic. " +
      "Set mode to 'enforce' to actively throttle, 'warn' to log without blocking, or 'off' to disable. " +
      "capacity is the max banked capacity (% of seconds of full server usage, 0-6000; unlimited when unset). " +
      "rate is the refill rate (% of server resources, 0-100; unlimited when unset). " +
      "burst is the max a single query can consume (0-6000; unlimited when unset). " +
      "concurrency is the % of available worker processes (0-100; unlimited when unset).",
    inputSchema: {
      organization: orgParam,
      database: dbParam,
      branch: branchParam,
      name: z.string().describe("Name of the traffic budget"),
      mode: z
        .enum(["enforce", "warn", "off"])
        .describe(
          "Budget mode: 'enforce' to throttle, 'warn' to log only, 'off' to disable",
        ),
      capacity: z
        .number()
        .optional()
        .describe(
          "Max banked capacity (0-6000, % of seconds of full server usage)",
        ),
      rate: z
        .number()
        .optional()
        .describe("Capacity refill rate (0-100, % of server resources)"),
      burst: z
        .number()
        .optional()
        .describe("Max capacity a single query can consume (0-6000)"),
      concurrency: z
        .number()
        .optional()
        .describe("Max % of available worker processes (0-100)"),
      rules: z
        .array(z.string())
        .optional()
        .describe("Array of traffic rule IDs to attach to this budget"),
    },
    async execute(ctx, input) {
      try {
        const env = getEnv(ctx);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }
        const authHeader = getAuthHeader(env);
        const result = await createTrafficBudget(
          input.organization,
          input.database,
          input.branch,
          {
            name: input.name,
            mode: input.mode,
            capacity: input.capacity,
            rate: input.rate,
            burst: input.burst,
            concurrency: input.concurrency,
            rules: input.rules,
          },
          authHeader,
        );
        return ctx.json(result);
      } catch (error) {
        return handleError(ctx, error);
      }
    },
  })
  .tool({
    name: "update_traffic_budget",
    description:
      "Update an existing traffic control budget. Any fields not provided are left unchanged.",
    inputSchema: {
      organization: orgParam,
      database: dbParam,
      branch: branchParam,
      id: z.string().describe("The ID of the traffic budget to update"),
      name: z.string().optional().describe("New name for the budget"),
      mode: z
        .enum(["enforce", "warn", "off"])
        .optional()
        .describe(
          "Budget mode: 'enforce' to throttle, 'warn' to log only, 'off' to disable",
        ),
      capacity: z
        .number()
        .optional()
        .describe(
          "Max banked capacity (0-6000, % of seconds of full server usage)",
        ),
      rate: z
        .number()
        .optional()
        .describe("Capacity refill rate (0-100, % of server resources)"),
      burst: z
        .number()
        .optional()
        .describe("Max capacity a single query can consume (0-6000)"),
      concurrency: z
        .number()
        .optional()
        .describe("Max % of available worker processes (0-100)"),
      rules: z
        .array(z.string())
        .optional()
        .describe("Array of traffic rule IDs to apply to the budget"),
    },
    async execute(ctx, input) {
      try {
        const env = getEnv(ctx);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }
        const authHeader = getAuthHeader(env);
        const body: Record<string, unknown> = {};
        if (input["name"] !== undefined) body["name"] = input["name"];
        if (input["mode"] !== undefined) body["mode"] = input["mode"];
        if (input["capacity"] !== undefined)
          body["capacity"] = input["capacity"];
        if (input["rate"] !== undefined) body["rate"] = input["rate"];
        if (input["burst"] !== undefined) body["burst"] = input["burst"];
        if (input["concurrency"] !== undefined)
          body["concurrency"] = input["concurrency"];
        if (input["rules"] !== undefined) body["rules"] = input["rules"];
        const result = await updateTrafficBudget(
          input.organization,
          input.database,
          input.branch,
          input.id,
          body,
          authHeader,
        );
        return ctx.json(result);
      } catch (error) {
        return handleError(ctx, error);
      }
    },
  })
  .tool({
    name: "delete_traffic_budget",
    description: "Delete a traffic control budget from a branch.",
    inputSchema: {
      organization: orgParam,
      database: dbParam,
      branch: branchParam,
      id: z.string().describe("The ID of the traffic budget to delete"),
    },
    async execute(ctx, input) {
      try {
        const env = getEnv(ctx);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }
        const authHeader = getAuthHeader(env);
        await deleteTrafficBudget(
          input.organization,
          input.database,
          input.branch,
          input.id,
          authHeader,
        );
        return ctx.json({ success: true });
      } catch (error) {
        return handleError(ctx, error);
      }
    },
  })
  .tool({
    name: "add_traffic_rule",
    description:
      "Add a traffic rule to a budget. Rules match query traffic to a budget " +
      "using tags (key/value pairs from SQL comments or system metadata) and/or a query fingerprint.",
    inputSchema: {
      organization: orgParam,
      database: dbParam,
      branch: branchParam,
      budget_id: z
        .string()
        .describe("The ID of the traffic budget to add the rule to"),
      kind: z
        .enum(["match"])
        .describe("Rule kind (currently only 'match' is supported)"),
      tags: z
        .array(
          z.object({
            key: z.string().describe("Tag key"),
            value: z.string().describe("Tag value"),
          }),
        )
        .optional()
        .describe(
          "Tags to match against (from SQL comments or system metadata)",
        ),
      fingerprint: z
        .string()
        .optional()
        .describe("Query fingerprint to target with this rule"),
    },
    async execute(ctx, input) {
      try {
        const env = getEnv(ctx);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }
        const authHeader = getAuthHeader(env);
        const result = await createTrafficRule(
          input.organization,
          input.database,
          input.branch,
          input.budget_id,
          {
            kind: input.kind,
            tags: input.tags,
            fingerprint: input.fingerprint,
          },
          authHeader,
        );
        return ctx.json(result);
      } catch (error) {
        return handleError(ctx, error);
      }
    },
  })
  .tool({
    name: "delete_traffic_rule",
    description: "Delete a traffic rule from a budget.",
    inputSchema: {
      organization: orgParam,
      database: dbParam,
      branch: branchParam,
      budget_id: z.string().describe("The ID of the traffic budget"),
      id: z.string().describe("The ID of the traffic rule to delete"),
    },
    async execute(ctx, input) {
      try {
        const env = getEnv(ctx);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }
        const authHeader = getAuthHeader(env);
        await deleteTrafficRule(
          input.organization,
          input.database,
          input.branch,
          input.budget_id,
          input.id,
          authHeader,
        );
        return ctx.json({ success: true });
      } catch (error) {
        return handleError(ctx, error);
      }
    },
  });
