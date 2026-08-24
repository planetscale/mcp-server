import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { paymentMethodsGram } from "./payment-methods.ts";

const originalFetch = globalThis.fetch;
const originalToken = process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"];

afterEach(() => {
  globalThis.fetch = originalFetch;
  if (originalToken === undefined) {
    delete process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"];
  } else {
    process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = originalToken;
  }
});

test("update_payment_method creates Checkout and explains the next action", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  globalThis.fetch = async (input, init) => {
    assert.equal(
      input.toString(),
      "https://api.planetscale.com/v1/organizations/acme/billing/payment-method-setups"
    );
    assert.equal(init?.method, "POST");
    assert.equal(
      (init?.headers as Record<string, string>)["Authorization"],
      "Bearer oauth-token"
    );
    return Response.json(
      {
        id: "pmsetup1",
        state: "pending",
        checkout_url: "https://checkout.stripe.com/test",
        expires_at: "2026-08-24T21:00:00Z",
      },
      { status: 201 }
    );
  };

  const response = await paymentMethodsGram.handleToolCall({
    name: "update_payment_method",
    input: { organization: "acme" },
  });
  const result = (await response.json()) as {
    setup_id: string;
    checkout_url: string;
    next_action: {
      tool: string;
      arguments: { organization: string; setup_id: string };
    };
  };

  assert.equal(result.setup_id, "pmsetup1");
  assert.equal(result.checkout_url, "https://checkout.stripe.com/test");
  assert.deepEqual(result.next_action, {
    tool: "get_update_payment_method_status",
    arguments: { organization: "acme", setup_id: "pmsetup1" },
    instruction:
      "Give checkout_url to the customer. After they finish Stripe Checkout, call the status tool with this same setup_id. Do not create another setup while this one is pending.",
  });
});

test("status keeps a pending setup resumable without creating another", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  globalThis.fetch = async (input, init) => {
    assert.equal(
      input.toString(),
      "https://api.planetscale.com/v1/organizations/acme/billing/payment-method-setups/pmsetup1"
    );
    assert.equal(init?.method, undefined);
    return Response.json({
      id: "pmsetup1",
      state: "pending",
      checkout_url: "https://checkout.stripe.com/test",
    });
  };

  const response = await paymentMethodsGram.handleToolCall({
    name: "get_update_payment_method_status",
    input: { organization: "acme", setup_id: "pmsetup1" },
  });
  const result = (await response.json()) as {
    state: string;
    next_action: { tool: string };
  };

  assert.equal(result.state, "pending");
  assert.equal(result.next_action.tool, "get_update_payment_method_status");
});

test("status returns saved card details after Checkout completes", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const requests: string[] = [];
  globalThis.fetch = async (input) => {
    requests.push(input.toString());
    if (requests.length === 1) {
      return Response.json({
        id: "pmsetup1",
        state: "completed",
        completed_at: "2026-08-24T20:30:00Z",
      });
    }
    return Response.json({
      id: "pm_123",
      brand: "visa",
      last4: "4242",
      exp_month: 12,
      exp_year: 2030,
      name: "Test Customer",
    });
  };

  const response = await paymentMethodsGram.handleToolCall({
    name: "get_update_payment_method_status",
    input: { organization: "acme", setup_id: "pmsetup1" },
  });
  const result = (await response.json()) as {
    state: string;
    message: string;
    payment_method: { brand: string; last4: string };
    next_action: null;
  };

  assert.deepEqual(requests, [
    "https://api.planetscale.com/v1/organizations/acme/billing/payment-method-setups/pmsetup1",
    "https://api.planetscale.com/v1/organizations/acme/billing/payment-method",
  ]);
  assert.equal(result.state, "completed");
  assert.equal(
    result.message,
    "The organization's payment method was updated successfully."
  );
  assert.deepEqual(result.payment_method, {
    id: "pm_123",
    brand: "visa",
    last4: "4242",
    exp_month: 12,
    exp_year: 2030,
    name: "Test Customer",
  });
  assert.equal(result.next_action, null);
});

test("status keeps Checkout completed when card confirmation lacks read_payment_method", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const requests: string[] = [];
  globalThis.fetch = async (input) => {
    requests.push(input.toString());
    if (requests.length === 1) {
      return Response.json({
        id: "pmsetup1",
        state: "completed",
        completed_at: "2026-08-24T20:30:00Z",
      });
    }
    return Response.json(
      { code: "forbidden", message: "User does not have permission to perform this action." },
      { status: 403 }
    );
  };

  const response = await paymentMethodsGram.handleToolCall({
    name: "get_update_payment_method_status",
    input: { organization: "acme", setup_id: "pmsetup1" },
  });
  const result = (await response.json()) as {
    state: string;
    payment_method: null;
    warning: string;
    next_action: null;
  };

  assert.deepEqual(requests, [
    "https://api.planetscale.com/v1/organizations/acme/billing/payment-method-setups/pmsetup1",
    "https://api.planetscale.com/v1/organizations/acme/billing/payment-method",
  ]);
  assert.equal(result.state, "completed");
  assert.equal(result.payment_method, null);
  assert.equal(
    result.warning,
    "Checkout succeeded, but confirming the saved card requires read_payment_method."
  );
  assert.equal(result.next_action, null);
});
