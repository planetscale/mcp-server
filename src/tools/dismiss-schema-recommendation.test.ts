import assert from "node:assert/strict";
import test from "node:test";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { dismissSchemaRecommendation } from "./dismiss-schema-recommendation.ts";

test("dismissSchemaRecommendation posts the reason to the recommendation endpoint", async (t) => {
  let requestUrl = "";
  let requestInit: RequestInit | undefined;
  t.mock.method(
    globalThis,
    "fetch",
    async (...args: Parameters<typeof fetch>): Promise<Response> => {
      requestUrl = args[0].toString();
      requestInit = args[1];
      return Response.json({
        id: "rec-123",
        number: 42,
        state: "dismissed",
        html_dismissed_reason: "<p>Covered by a composite index</p>",
      });
    }
  );

  const response = await dismissSchemaRecommendation(
    "my org",
    "game/db",
    42,
    "Covered by a composite index",
    "Bearer token"
  );

  assert.equal(
    new URL(requestUrl).pathname,
    "/v1/organizations/my%20org/databases/game%2Fdb/schema-recommendations/42/dismiss"
  );
  assert.equal(requestInit?.method, "POST");
  assert.equal(
    new Headers(requestInit?.headers).get("Authorization"),
    "Bearer token"
  );
  assert.deepEqual(JSON.parse(requestInit?.body as string), {
    reason: "Covered by a composite index",
  });
  assert.equal(response.state, "dismissed");
});

test("dismissSchemaRecommendation permits dismissing without a reason", async (t) => {
  let requestBody = "";
  t.mock.method(
    globalThis,
    "fetch",
    async (...args: Parameters<typeof fetch>): Promise<Response> => {
      requestBody = args[1]?.body as string;
      return Response.json({
        id: "rec-123",
        number: 42,
        state: "dismissed",
        dismissed_at: "2026-08-02T20:00:00Z",
      });
    }
  );

  const response = await dismissSchemaRecommendation(
    "org",
    "db",
    42,
    undefined,
    "Bearer token"
  );

  assert.deepEqual(JSON.parse(requestBody), {});
  assert.equal(response.state, "dismissed");
});

test("dismissSchemaRecommendation preserves API permission errors", async (t) => {
  t.mock.method(globalThis, "fetch", async (): Promise<Response> => {
    return Response.json(
      { error: "missing write_database access" },
      { status: 403, statusText: "Forbidden" }
    );
  });

  await assert.rejects(
    dismissSchemaRecommendation(
      "org",
      "db",
      7,
      undefined,
      "Bearer read-only-token"
    ),
    (error: unknown) => {
      assert.ok(error instanceof PlanetScaleAPIError);
      assert.equal(error.statusCode, 403);
      assert.match(error.message, /Permission denied/);
      return true;
    }
  );
});
