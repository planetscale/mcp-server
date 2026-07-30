/**
 * Format byte count to human-readable string (e.g. 1073741824 -> "1 GB")
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / Math.pow(1024, i);
  const unit = units[Math.min(i, units.length - 1)];
  return value % 1 === 0 ? `${value} ${unit}` : `${value.toFixed(1)} ${unit}`;
}
