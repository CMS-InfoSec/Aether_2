const DEFAULT_OPTIONS: Intl.DateTimeFormatOptions = {
  day: "2-digit",
  month: "short",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "Europe/London",
};

const defaultFormatter = new Intl.DateTimeFormat("en-GB", DEFAULT_OPTIONS);

export type DateLike = string | number | Date | null | undefined;

function toDate(value: DateLike): Date | null {
  if (value === null || value === undefined) {
    return null;
  }

  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed;
}

export function formatLondonTime(
  value: DateLike,
  options?: Intl.DateTimeFormatOptions,
  fallback?: string
): string {
  const date = toDate(value);

  if (!date) {
    if (fallback !== undefined) {
      return fallback;
    }

    return typeof value === "string" ? value : "";
  }

  if (!options) {
    return defaultFormatter.format(date);
  }

  return new Intl.DateTimeFormat("en-GB", {
    ...DEFAULT_OPTIONS,
    ...options,
    timeZone: "Europe/London",
  }).format(date);
}
