/** Canonical cross-process Minutes identity is the positive PostgreSQL row id, never a native link. */
const MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807n;

export function canonicalMeetingRow(value: unknown): string | null {
  const row = typeof value === "number" && Number.isSafeInteger(value) ? String(value) : value;
  if (typeof row !== "string" || !/^[1-9]\d{0,18}$/.test(row)) return null;
  try {
    return BigInt(row) <= MAX_POSTGRES_BIGINT ? row : null;
  } catch {
    return null;
  }
}

export function canonicalMeetingDocPath(value: unknown): string | null {
  const row = canonicalMeetingRow(value);
  return row ? `kg/entities/meeting/${row}.md` : null;
}
