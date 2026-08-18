# Cluster sizes (`list_cluster_sizes`)

Source: `src/tools/list-cluster-sizes.ts`

Lists the cluster size tiers an organization can pick from, with prices. Raw
SKUs from the API get deduplicated by display name and folded into one entry per
tier, with rates split per CPU architecture and byte counts formatted for
reading.

## Reach it

A user asks what a database will cost, or which sizes exist, before creating or
resizing one.

## Drive it

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_cluster_sizes \
  '{"organization":"YOUR_ORG","engine":"mysql"}' \
  --expect '"cluster_sizes"' --label sizes-mysql
```

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_cluster_sizes \
  '{"organization":"YOUR_ORG","engine":"postgresql","type":"metal"}' \
  --expect '"type":"metal"' --label sizes-metal
```

## Proves it works

- `cluster_sizes` is non-empty and `total_tiers` matches its length.
- Each tier reads as a human would expect: `cpu` like `"1 vCPU"`, `ram` like
  `"4 GB"`, rates like `"$39/mo"` — not raw byte counts or bare numbers.
- `type: "metal"` returns only M-* tiers, each with `storage_options`;
  `type: "autoscaling"` returns only PS-* tiers, each with
  `storage: "autoscaling (network-backed)"`.
- Tiers come back in `sort_order`, smallest first.

## Notes

- This endpoint needs organization-level read scope. A service token that can
  list databases may still 403 here. Confirm the token's scopes before treating
  a 403 as a regression.
- The API has been seen returning both a bare array and a `{ data: [...] }`
  wrapper. `fetchClusterSizeSkus` accepts either, so changes there should be
  checked against a live response rather than a fixture.
