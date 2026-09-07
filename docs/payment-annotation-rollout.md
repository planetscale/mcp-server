# Payment annotation rollout

`update_payment_method` creates a Stripe Checkout setup; the customer completes
any card change in their browser. We conservatively retain the hosted
`destructiveHint: true` classification while Claude directory review is pending.
The other hints remain `readOnlyHint: false` and `openWorldHint: true`.

## Remove the temporary dashboard override after deployment

Keep the current payment annotation override enabled in Gram until this change
has been reviewed, merged, and deployed. Creating this draft does not deploy it.

After deployment:

1. Confirm the deployed `mcp-server` function manifest declares the values above.
2. Record the payment tool's current dashboard values, then remove its annotation
   override so it inherits the source definition. Do **not** switch Destructive
   off: a saved `false` value is another override, not inheritance.
3. Fetch a fresh authenticated `tools/list` response and confirm
   `planetscale_update_payment_method` still returns `readOnlyHint: false`,
   `destructiveHint: true`, and `openWorldHint: true`.
4. Have a workspace admin run Claude's connector review checks. Restore the
   recorded override if the effective values change unexpectedly or validation
   fails, and investigate before retrying removal.

The final state should inherit these annotations from `mcp-server` code, with no
payment annotation override in the dashboard. If Gram's editor cannot clear an
override, use a supported reset/delete-variation path; do not substitute an
unchecked switch.
