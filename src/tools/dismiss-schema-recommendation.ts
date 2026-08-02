import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { getAuthHeader, getAuthToken } from "../lib/auth.ts";
import {
  PlanetScaleAPIError,
  apiRequest,
} from "../lib/planetscale-api.ts";

export interface SchemaRecommendation {
  id: string;
  number: number;
  state: string;
  dismissed_at?: string | null;
  html_dismissed_reason?: string | null;
  [key: string]: unknown;
}

function schemaRecommendationPath(
  organization: string,
  database: string,
  recommendationNumber: number
): string {
  return `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/schema-recommendations/${encodeURIComponent(recommendationNumber.toString())}/dismiss`;
}

export async function dismissSchemaRecommendation(
  organization: string,
  database: string,
  recommendationNumber: number,
  reason: string | undefined,
  authHeader: string
): Promise<SchemaRecommendation> {
  return apiRequest<SchemaRecommendation>(
    schemaRecommendationPath(organization, database, recommendationNumber),
    authHeader,
    {
      method: "POST",
      body: JSON.stringify(reason === undefined ? {} : { reason }),
    }
  );
}

function environment(value: object): Record<string, string | undefined> {
  return Object.keys(value).length > 0
    ? (value as Record<string, string | undefined>)
    : process.env;
}

function errorMessage(error: unknown): string {
  if (error instanceof PlanetScaleAPIError) {
    return `Error: ${error.message} (status: ${error.statusCode})`;
  }

  if (error instanceof Error) {
    return `Error: ${error.message}`;
  }

  return "Error: An unexpected error occurred";
}

export const dismissSchemaRecommendationGram = new Gram().tool({
  name: "dismiss_schema_recommendation",
  description:
    "Dismiss one PlanetScale schema recommendation as a false positive or otherwise not applicable. Only use this write action when the user explicitly asks to dismiss the recommendation. Call list_schema_recommendations first to obtain its number. An optional reason is retained with the dismissed recommendation.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    recommendation_number: z
      .number()
      .int()
      .positive()
      .describe("Recommendation number returned by list_schema_recommendations"),
    reason: z
      .string()
      .trim()
      .min(1)
      .max(500)
      .optional()
      .describe(
        "Why the recommendation is being dismissed (optional, maximum 500 characters)"
      ),
  },
  async execute(ctx, input) {
    try {
      const env = environment(ctx.env);
      if (!getAuthToken(env)) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const result = await dismissSchemaRecommendation(
        input.organization,
        input.database,
        input.recommendation_number,
        input.reason,
        getAuthHeader(env)
      );

      return ctx.json(result);
    } catch (error) {
      return ctx.text(errorMessage(error));
    }
  },
});
