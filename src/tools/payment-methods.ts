import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { getAuthHeader, getAuthToken } from "../lib/auth.ts";
import {
  createBillingPaymentMethodSetup,
  getBillingPaymentMethod,
  getBillingPaymentMethodSetup,
  PlanetScaleAPIError,
  type BillingPaymentMethodSetup,
} from "../lib/planetscale-api.ts";

const STATUS_TOOL = "get_payment_method_setup";
const UPDATE_TOOL = "update_payment_method";

function environment(
  contextEnvironment: Record<string, unknown>
): Record<string, string | undefined> {
  return Object.keys(contextEnvironment).length > 0
    ? (contextEnvironment as Record<string, string | undefined>)
    : process.env;
}

function apiErrorMessage(error: PlanetScaleAPIError): string {
  if (error.details && typeof error.details === "object") {
    const details = error.details as Record<string, unknown>;
    const detail = details["message"] ?? details["error"];
    if (typeof detail === "string" && detail.length > 0) {
      return detail;
    }
  }
  return error.message;
}

function paymentMethodConfirmationWarning(error: unknown): string {
  if (
    error instanceof PlanetScaleAPIError &&
    (error.statusCode === 401 || error.statusCode === 403)
  ) {
    return "Checkout succeeded, but confirming the saved card requires full access to the organization's payment method. Ask the customer to re-authorize with Payment method set to Full access.";
  }
  if (error instanceof PlanetScaleAPIError) {
    return apiErrorMessage(error);
  }
  return "The saved payment method details could not be loaded.";
}

function setupNextAction(
  organization: string,
  setup: BillingPaymentMethodSetup
) {
  if (setup.state === "pending") {
    return {
      tool: STATUS_TOOL,
      arguments: { organization, setup_id: setup.id },
      instruction:
        "Give checkout_url to the customer. After they finish Stripe Checkout, call the status tool with this same setup_id. Do not create another setup while this one is pending.",
    };
  }

  if (setup.state === "failed" || setup.state === "expired") {
    return {
      tool: UPDATE_TOOL,
      arguments: { organization },
      instruction:
        "This Checkout session cannot be resumed. Ask the customer before starting a new payment method update.",
    };
  }

  return null;
}

function setupMessage(setup: BillingPaymentMethodSetup): string {
  switch (setup.state) {
    case "pending":
      return "Stripe Checkout is waiting for the customer.";
    case "completed":
      return "The organization's payment method was updated successfully.";
    case "failed":
      return setup.error
        ? `The payment method update failed: ${setup.error}`
        : "The payment method update failed.";
    case "expired":
      return "The Stripe Checkout session expired before it was completed.";
  }
}

export const paymentMethodsGram = new Gram()
  .tool({
    name: UPDATE_TOOL,
    description:
      "Start a secure Stripe Checkout session to add or replace an organization's billing card. Use when a customer asks to add, set, replace, or update their PlanetScale payment method. This tool never collects card details: return checkout_url to the customer and let them complete Stripe Checkout in their browser. It returns a setup_id; do not call this tool again while that setup is pending. After the customer finishes, call get_payment_method_setup with the same organization and setup_id. Requires full access to the organization's payment method.",
    annotations: {
      title: "Start billing card setup",
      readOnlyHint: false,
      destructiveHint: false,
      openWorldHint: true,
    },
    inputSchema: {
      organization: z.string().describe("PlanetScale organization name"),
    },
    async execute(ctx, input) {
      try {
        const env = environment(ctx.env);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }

        const organization = input["organization"];
        const setup = await createBillingPaymentMethodSetup(
          organization,
          getAuthHeader(env),
          ctx.signal
        );

        if (!setup.id || !setup.checkout_url) {
          return ctx.text(
            "Error: PlanetScale created a payment method setup without an ID or Checkout URL."
          );
        }

        return ctx.json({
          organization,
          setup_id: setup.id,
          state: setup.state,
          checkout_url: setup.checkout_url,
          expires_at: setup.expires_at ?? null,
          message: setupMessage(setup),
          next_action: setupNextAction(organization, setup),
        });
      } catch (error) {
        if (error instanceof PlanetScaleAPIError) {
          return ctx.text(
            `Error: ${apiErrorMessage(error)} (status: ${error.statusCode})`
          );
        }
        if (error instanceof Error) {
          return ctx.text(`Error: ${error.message}`);
        }
        return ctx.text("Error: An unexpected error occurred");
      }
    },
  })
  .tool({
    name: STATUS_TOOL,
    description:
      "Get a Stripe Checkout payment method setup started by update_payment_method. Use the setup_id returned by that tool; it is not a payment method ID. This is a one-time check and does not wait. If pending, give checkout_url to the customer and call this tool again only after they finish Checkout. If completed, this tool also returns the saved card brand, last four digits, expiration, and cardholder name as confirmation. Failed or expired setups cannot be resumed. Requires full access to the organization's payment method to inspect the setup and return the saved card.",
    annotations: {
      title: "Get billing card setup status",
      readOnlyHint: true,
      destructiveHint: false,
      openWorldHint: false,
    },
    inputSchema: {
      organization: z.string().describe("PlanetScale organization name"),
      setup_id: z
        .string()
        .describe(
          "Payment method setup ID returned by update_payment_method, not a card or payment method ID"
        ),
    },
    async execute(ctx, input) {
      try {
        const env = environment(ctx.env);
        if (!getAuthToken(env)) {
          return ctx.text("Error: No PlanetScale authentication configured.");
        }

        const organization = input["organization"];
        const authHeader = getAuthHeader(env);
        const setup = await getBillingPaymentMethodSetup(
          organization,
          input["setup_id"],
          authHeader,
          ctx.signal
        );

        if (setup.state !== "completed") {
          return ctx.json({
            organization,
            setup_id: setup.id,
            state: setup.state,
            checkout_url: setup.checkout_url ?? null,
            expires_at: setup.expires_at ?? null,
            failed_at: setup.failed_at ?? null,
            error: setup.error ?? null,
            message: setupMessage(setup),
            next_action: setupNextAction(organization, setup),
          });
        }

        try {
          const paymentMethod = await getBillingPaymentMethod(
            organization,
            authHeader,
            ctx.signal
          );
          return ctx.json({
            organization,
            setup_id: setup.id,
            state: setup.state,
            completed_at: setup.completed_at ?? null,
            message: setupMessage(setup),
            payment_method: paymentMethod,
            next_action: null,
          });
        } catch (error) {
          return ctx.json({
            organization,
            setup_id: setup.id,
            state: setup.state,
            completed_at: setup.completed_at ?? null,
            message: setupMessage(setup),
            payment_method: null,
            warning: paymentMethodConfirmationWarning(error),
            next_action: null,
          });
        }
      } catch (error) {
        if (error instanceof PlanetScaleAPIError) {
          return ctx.text(
            `Error: ${apiErrorMessage(error)} (status: ${error.statusCode})`
          );
        }
        if (error instanceof Error) {
          return ctx.text(`Error: ${error.message}`);
        }
        return ctx.text("Error: An unexpected error occurred");
      }
    },
  });
