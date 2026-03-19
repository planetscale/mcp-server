import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface DeployOperationRaw {
  id?: string;
  state?: string;
  keyspace_name?: string;
  table_name?: string;
  operation_name?: string;
  ddl_statement?: string;
  eta_seconds?: number;
  progress_percentage?: number;
  can_drop_data?: boolean;
  deploy_errors?: string | null;
  created_at?: string;
  updated_at?: string;
}

interface PaginatedResponse {
  data: DeployOperationRaw[];
  current_page: number;
  next_page: number | null;
  prev_page: number | null;
}

function filterOperation(op: DeployOperationRaw) {
  return {
    id: op.id,
    state: op.state,
    keyspace_name: op.keyspace_name,
    table_name: op.table_name,
    operation_name: op.operation_name,
    ddl_statement: op.ddl_statement,
    eta_seconds: op.eta_seconds,
    progress_percentage: op.progress_percentage,
    can_drop_data: op.can_drop_data,
    deploy_errors: op.deploy_errors,
    created_at: op.created_at,
    updated_at: op.updated_at,
  };
}

export const listDeployOperationsGram = new Gram().tool({
  name: "list_deploy_operations",
  description:
    "List the individual DDL operations (schema changes) within a deploy request. Each operation represents a single schema change (e.g., ALTER TABLE, CREATE INDEX). Use this to understand exactly what schema changes a deploy request will apply.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    number: z.number().describe("Deploy request number"),
    page: z.number().optional().describe("Page number (default: 1)"),
    per_page: z
      .number()
      .optional()
      .describe("Results per page (default: 25, max: 50)"),
  },
  async execute(ctx, input) {
    try {
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

      const auth = getAuthToken(env);
      if (!auth) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const organization = input["organization"];
      const database = input["database"];
      const number = input["number"];

      if (!organization || !database || number == null) {
        return ctx.text(
          "Error: organization, database, and number are required"
        );
      }

      const authHeader = getAuthHeader(env);
      const page = input["page"] ?? 1;
      const perPage = Math.min(input["per_page"] ?? 25, 50);

      const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/deploy-requests/${encodeURIComponent(String(number))}/operations?page=${page}&per_page=${perPage}`;

      const response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: authHeader,
          Accept: "application/json",
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
            "Deploy request not found. Please check your organization, database, and deploy request number.",
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
          `Failed to list deploy operations: ${response.statusText}`,
          response.status,
          details
        );
      }

      const result = (await response.json()) as PaginatedResponse;
      const filtered = (result.data || []).map(filterOperation);

      return ctx.json({
        organization,
        database,
        deploy_request_number: number,
        page: result.current_page,
        next_page: result.next_page,
        total: filtered.length,
        operations: filtered,
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(
          `Error: ${error.message} (status: ${error.statusCode})`
        );
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
