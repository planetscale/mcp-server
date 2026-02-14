import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface ClusterSizeSkuRaw {
  name: string;
  type: string;
  display_name: string;
  cpu: string;
  provider_instance_type: string | null;
  storage: number | null;
  storage_name: string | null;
  ram: number;
  sort_order: number;
  architecture: string;
  development: boolean;
  production: boolean;
  metal: boolean;
  enabled: boolean;
  provider: string;
  rate: number | null;
  replica_rate: number | null;
  default_vtgate?: string;
  default_vtgate_rate?: number | null;
}

export interface TierSummary {
  name: string;
  type: "autoscaling" | "metal";
  cpu: string;
  ram: string;
  providers: string[];
  rate: string | null;
  replica_rate: string | null;
  storage?: string;
  storage_options?: string[];
}

/**
 * Format byte count to human-readable string (e.g. 1073741824 -> "1 GB")
 */
function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / Math.pow(1024, i);
  const unit = units[Math.min(i, units.length - 1)];
  return value % 1 === 0 ? `${value} ${unit}` : `${value.toFixed(1)} ${unit}`;
}

/**
 * Format CPU string for display (e.g. "1" -> "1 vCPU", "1/2" -> "0.5 vCPU")
 */
function formatCpu(cpu: string): string {
  if (cpu.includes("/")) {
    const parts = cpu.split("/").map(Number);
    const num = parts[0];
    const den = parts[1];
    if (num === undefined || den === undefined || den === 0) return `${cpu} vCPU`;
    const value = num / den;
    return value === 1 ? "1 vCPU" : `${value} vCPU`;
  }
  return `${cpu} vCPU`;
}

/**
 * Format rate (cents or dollars) for display
 */
function formatRate(rate: number | null): string | null {
  if (rate == null) return null;
  const dollars = rate / 100;
  return `$${dollars}/mo`;
}

/**
 * Fetch cluster size SKUs from the PlanetScale API
 */
async function fetchClusterSizeSkus(
  organization: string,
  engine: "mysql" | "postgresql",
  authHeader: string
): Promise<ClusterSizeSkuRaw[]> {
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/cluster_size_skus?engine=${engine}&rates=true`;

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
        "Cluster size SKUs not found. Please check your organization name.",
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
      `Failed to fetch cluster size SKUs: ${response.statusText}`,
      response.status,
      details
    );
  }

  const data = (await response.json()) as ClusterSizeSkuRaw[];
  return Array.isArray(data) ? data : [];
}

/**
 * Deduplicate SKUs by display_name and build compact tier summaries
 */
function buildTierSummaries(
  skus: ClusterSizeSkuRaw[],
  typeFilter: "autoscaling" | "metal" | undefined
): TierSummary[] {
  const byTier = new Map<
    string,
    {
      sortOrder: number;
      cpu: string;
      ram: string;
      providers: Set<string>;
      storageBytes: Set<number>;
      rate: number | null;
      replicaRate: number | null;
      metal: boolean;
    }
  >();

  for (const sku of skus) {
    if (!sku.enabled) continue;
    const isMetal = sku.metal;
    if (typeFilter === "autoscaling" && isMetal) continue;
    if (typeFilter === "metal" && !isMetal) continue;

    const key = sku.display_name;
    let tier = byTier.get(key);
    if (!tier) {
      tier = {
        sortOrder: sku.sort_order,
        cpu: sku.cpu,
        ram: formatBytes(sku.ram),
        providers: new Set<string>(),
        storageBytes: new Set<number>(),
        rate: sku.rate ?? null,
        replicaRate: sku.replica_rate ?? null,
        metal: isMetal,
      };
      byTier.set(key, tier);
    }

    tier.providers.add(sku.provider);
    if (isMetal && sku.storage != null && sku.storage > 0) {
      tier.storageBytes.add(sku.storage);
    }
    if (sku.rate != null && tier.rate == null) tier.rate = sku.rate;
    if (sku.replica_rate != null && tier.replicaRate == null)
      tier.replicaRate = sku.replica_rate;
  }

  const result: TierSummary[] = [];
  const entries = Array.from(byTier.entries()).sort(
    (a, b) => a[1].sortOrder - b[1].sortOrder
  );
  for (const [displayName, tier] of entries) {
    const summary: TierSummary = {
      name: displayName,
      type: tier.metal ? "metal" : "autoscaling",
      cpu: formatCpu(tier.cpu),
      ram: tier.ram,
      providers: Array.from(tier.providers).sort(),
      rate: formatRate(tier.rate)
        ? `${formatRate(tier.rate)} (HA cluster)`
        : null,
      replica_rate: formatRate(tier.replicaRate)
        ? `${formatRate(tier.replicaRate)} (single instance)`
        : null,
    };
    if (tier.metal && tier.storageBytes.size > 0) {
      summary.storage_options = Array.from(tier.storageBytes)
        .sort((a, b) => a - b)
        .map(formatBytes);
    } else if (!tier.metal) {
      summary.storage = "autoscaling (network-backed)";
    }
    result.push(summary);
  }

  return result;
}

export const listClusterSizesGram = new Gram().tool({
  name: "list_cluster_sizes",
  description:
    "List available PlanetScale cluster sizes (SKUs) for an organization. PS-* sizes use autoscaling network-backed storage; M-* sizes use super fast NVMe storage drives. The rate field is for an HA cluster with 2 replicas; replica_rate is for a single instance. Single instance databases are only available for Postgres. Metal instances must be HA.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    engine: z
      .enum(["mysql", "postgresql"])
      .optional()
      .describe("Database engine to list SKUs for (default: mysql)"),
    type: z
      .enum(["autoscaling", "metal"])
      .optional()
      .describe(
        "Filter to only autoscaling (PS-*, network-backed) or metal (M-*, local storage) sizes"
      ),
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
      if (!organization) {
        return ctx.text("Error: organization is required.");
      }

      const engine = (input["engine"] ?? "mysql") as "mysql" | "postgresql";
      const typeFilter = input["type"] as "autoscaling" | "metal" | undefined;
      const authHeader = getAuthHeader(env);

      const skus = await fetchClusterSizeSkus(organization, engine, authHeader);
      const tiers = buildTierSummaries(skus, typeFilter);

      return ctx.json({
        organization,
        engine,
        total_tiers: tiers.length,
        cluster_sizes: tiers,
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
