import React, { useCallback, useEffect, useMemo, useState } from "react";
import { withErrorBoundary } from "./withErrorBoundary";
import { formatLondonTime } from "./timezone";

type AuditLogEntry = {
  id?: string;
  actor: string;
  action: string;
  entity: string;
  ts: string;
  hash?: string;
  prev_hash?: string | null;
};

type IntegrityStatus = "valid" | "invalid" | "unknown";

type ValidatedAuditLogEntry = AuditLogEntry & {
  integrity: IntegrityStatus;
  anomalies: string[];
  warnings: string[];
  computedHash: string | null;
  expectedPrev: string | null;
};

const ensureString = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  return String(value);
};

const mapAuditEntry = (raw: unknown): AuditLogEntry | null => {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const record = raw as Record<string, unknown>;

  const actor = ensureString(
    record.actor ?? record.actor_id ?? record.user ?? record.principal ?? ""
  );
  const action = ensureString(record.action ?? record.event ?? record.type ?? "");
  const entity = ensureString(
    record.entity ??
      record.entity_id ??
      record.target ??
      record.resource ??
      record.subject ??
      record.object ??
      ""
  );
  const tsRaw = record.ts ?? record.timestamp ?? record.created_at ?? record.recorded_at ?? "";
  const ts = ensureString(tsRaw);
  const hash = ensureString(record.hash ?? "");
  const prevHashValue = record.prev_hash ?? record.previous_hash ?? null;
  const prev_hash =
    prevHashValue === null || prevHashValue === undefined
      ? null
      : ensureString(prevHashValue);
  const idValue = record.id ?? record.event_id ?? record.audit_id ?? null;
  const id = idValue === null || idValue === undefined ? undefined : ensureString(idValue);

  if (!actor && !action && !entity && !ts) {
    return null;
  }

  return {
    id,
    actor,
    action,
    entity: entity || "—",
    ts,
    hash,
    prev_hash,
  };
};

const extractAuditEntries = (payload: unknown): AuditLogEntry[] => {
  const tryExtractArray = (value: unknown): unknown[] | null => {
    if (!value) {
      return null;
    }
    if (Array.isArray(value)) {
      return value;
    }
    if (typeof value === "object") {
      const container = value as Record<string, unknown>;
      const candidateKeys = ["logs", "results", "data", "entries"] as const;
      for (const key of candidateKeys) {
        if (Array.isArray(container[key])) {
          return container[key] as unknown[];
        }
      }
    }
    return null;
  };

  const arrayPayload = tryExtractArray(payload) ?? [];
  return arrayPayload
    .map((item) => mapAuditEntry(item))
    .filter((entry): entry is AuditLogEntry => entry !== null);
};

const getSubtleCrypto = (): SubtleCrypto | null => {
  if (typeof window === "undefined") {
    return null;
  }
  const cryptoObject = window.crypto ?? null;
  if (!cryptoObject || !cryptoObject.subtle) {
    return null;
  }
  return cryptoObject.subtle;
};

const toHex = (buffer: ArrayBuffer): string => {
  const bytes = new Uint8Array(buffer);
  let output = "";
  for (let index = 0; index < bytes.length; index += 1) {
    output += bytes[index].toString(16).padStart(2, "0");
  }
  return output;
};

const canonicalString = (entry: AuditLogEntry): string =>
  [entry.actor, entry.action, entry.entity, entry.ts].join("|");

const verifyAuditChain = async (
  entries: AuditLogEntry[]
): Promise<ValidatedAuditLogEntry[]> => {
  if (!entries.length) {
    return [];
  }

  const subtle = getSubtleCrypto();
  const canVerifyHash = Boolean(subtle) && typeof TextEncoder !== "undefined";
  const encoder = canVerifyHash ? new TextEncoder() : null;

  const orderedEntries = entries.slice().sort((a, b) => {
    const aTime = Date.parse(a.ts);
    const bTime = Date.parse(b.ts);
    if (!Number.isNaN(aTime) && !Number.isNaN(bTime)) {
      return aTime - bTime;
    }
    return a.ts.localeCompare(b.ts);
  });

  const results: ValidatedAuditLogEntry[] = [];
  let previousHash: string | null = null;

  for (const entry of orderedEntries) {
    const actualPrev = entry.prev_hash ?? "";
    const expectedPrev = previousHash;
    const anomalies: string[] = [];
    const warnings: string[] = [];
    let computedHash: string | null = null;

    if (expectedPrev !== null && actualPrev !== (expectedPrev ?? "")) {
      anomalies.push(
        `prev_hash mismatch: expected ${expectedPrev || "∅"}, received ${actualPrev || "∅"}.`
      );
    }

    if (!entry.hash) {
      anomalies.push("Missing hash value on entry.");
    }

    if (canVerifyHash && entry.hash) {
      try {
        const digestMaterial = `${actualPrev}|${canonicalString(entry)}`;
        const data = encoder!.encode(digestMaterial);
        const digest = await subtle!.digest("SHA-256", data);
        computedHash = toHex(digest);
        if (entry.hash.toLowerCase() !== computedHash.toLowerCase()) {
          anomalies.push("Hash mismatch detected.");
        }
      } catch (error) {
        warnings.push("Failed to compute hash for verification.");
        computedHash = null;
      }
    } else if (!canVerifyHash) {
      warnings.push("Hash verification unavailable in this environment.");
    }

    const integrity: IntegrityStatus = anomalies.length
      ? "invalid"
      : canVerifyHash
      ? "valid"
      : "unknown";

    results.push({
      ...entry,
      integrity,
      anomalies,
      warnings,
      computedHash,
      expectedPrev,
    });

    previousHash = entry.hash ? entry.hash : previousHash;
  }

  return results;
};

const formatTimestamp = (value: string): string => {
  if (!value) {
    return "—";
  }
  const formatted = formatLondonTime(value);
  if (!formatted) {
    return value;
  }
  return formatted;
};

const toCsvValue = (value: string): string => `"${value.replace(/"/g, '""')}"`;

const AuditViewer: React.FC = () => {
  const [rawEntries, setRawEntries] = useState<AuditLogEntry[]>([]);
  const [validatedEntries, setValidatedEntries] = useState<ValidatedAuditLogEntry[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const fetchLogs = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch("/audit/query", {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
        signal,
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `Failed to load audit logs (status ${response.status}).`);
      }

      const data = await response.json();
      const entries = extractAuditEntries(data);
      setRawEntries(entries);
    } catch (cause) {
      if (signal?.aborted) {
        return;
      }
      console.error("Failed to fetch audit logs", cause);
      setError(
        cause instanceof Error
          ? cause.message
          : "An unexpected error occurred while loading audit logs."
      );
      setRawEntries([]);
    } finally {
      if (!signal?.aborted) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    fetchLogs(controller.signal).catch((cause) => {
      if (!controller.signal.aborted) {
        console.error("Audit log initialisation failed", cause);
      }
    });
    return () => controller.abort();
  }, [fetchLogs]);

  useEffect(() => {
    let cancelled = false;
    if (!rawEntries.length) {
      setValidatedEntries([]);
      return undefined;
    }
    (async () => {
      try {
        const validated = await verifyAuditChain(rawEntries);
        if (!cancelled) {
          setValidatedEntries(validated);
        }
      } catch (cause) {
        console.error("Failed to verify audit chain", cause);
        if (!cancelled) {
          setValidatedEntries(
            rawEntries.map((entry) => ({
              ...entry,
              integrity: "unknown",
              anomalies: ["Unable to verify chain integrity."],
              warnings: [],
              computedHash: null,
              expectedPrev: null,
            }))
          );
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [rawEntries]);

  const hasAnomalies = useMemo(
    () => validatedEntries.some((entry) => entry.integrity === "invalid"),
    [validatedEntries]
  );

  const handleRefresh = useCallback(() => {
    const controller = new AbortController();
    fetchLogs(controller.signal).finally(() => {
      controller.abort();
    });
  }, [fetchLogs]);

  const handleExportCsv = useCallback(() => {
    if (!validatedEntries.length) {
      return;
    }
    const header = [
      "Timestamp",
      "Actor",
      "Action",
      "Entity",
      "Hash",
      "Prev Hash",
      "Integrity",
      "Anomalies",
    ];
    const rows = validatedEntries.map((entry) => [
      formatTimestamp(entry.ts),
      entry.actor,
      entry.action,
      entry.entity,
      entry.hash ?? "",
      entry.prev_hash ?? "",
      entry.integrity,
      [...entry.anomalies, ...entry.warnings].join("; "),
    ]);
    const csvContent = [header, ...rows]
      .map((row) => row.map((value) => toCsvValue(value)).join(","))
      .join("\r\n");

    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `audit-log-${new Date().toISOString()}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }, [validatedEntries]);

  return (
    <div className="bg-white shadow rounded-lg p-6">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div>
          <h2 className="text-2xl font-semibold text-gray-900">Audit Log Viewer</h2>
          <p className="text-sm text-gray-500">
            Review tamper-evident audit entries and verify the integrity of the chained hash log.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={handleRefresh}
            className="inline-flex items-center rounded-md border border-gray-300 bg-white px-3 py-2 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2"
          >
            Refresh
          </button>
          <button
            type="button"
            onClick={handleExportCsv}
            disabled={!validatedEntries.length}
            className="inline-flex items-center rounded-md border border-transparent bg-indigo-600 px-3 py-2 text-sm font-medium text-white shadow-sm hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-indigo-300"
          >
            Export CSV
          </button>
        </div>
      </div>

      {loading && (
        <p className="text-sm text-gray-500" aria-live="polite">
          Loading audit logs…
        </p>
      )}

      {error && (
        <div className="mb-4 rounded-md border border-red-200 bg-red-50 p-3" role="alert">
          <p className="text-sm font-medium text-red-700">{error}</p>
        </div>
      )}

      {!loading && !error && !validatedEntries.length && (
        <p className="text-sm text-gray-500">No audit log entries available.</p>
      )}

      {validatedEntries.length > 0 && (
        <>
          {hasAnomalies ? (
            <div className="mb-4 rounded-md border border-red-200 bg-red-50 p-3" role="status">
              <p className="text-sm font-semibold text-red-700">
                Chain anomalies detected. Review highlighted entries below.
              </p>
            </div>
          ) : (
            <div className="mb-4 rounded-md border border-green-200 bg-green-50 p-3" role="status">
              <p className="text-sm font-semibold text-green-700">
                Chain integrity verified successfully.
              </p>
            </div>
          )}

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th
                    scope="col"
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"
                  >
                    Timestamp
                  </th>
                  <th
                    scope="col"
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"
                  >
                    Actor
                  </th>
                  <th
                    scope="col"
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"
                  >
                    Action
                  </th>
                  <th
                    scope="col"
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"
                  >
                    Entity
                  </th>
                  <th
                    scope="col"
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"
                  >
                    Integrity
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200 bg-white">
                {validatedEntries.map((entry) => {
                  const rowKey = entry.id ?? `${entry.actor}-${entry.action}-${entry.ts}`;
                  const isInvalid = entry.integrity === "invalid";
                  const integrityLabel =
                    entry.integrity === "valid"
                      ? "Chain intact"
                      : entry.integrity === "unknown"
                      ? "Verification unavailable"
                      : "Anomaly detected";
                  const statusClass =
                    entry.integrity === "valid"
                      ? "text-green-700"
                      : entry.integrity === "unknown"
                      ? "text-yellow-700"
                      : "text-red-700";
                  const statusBadgeClass =
                    entry.integrity === "valid"
                      ? "bg-green-100"
                      : entry.integrity === "unknown"
                      ? "bg-yellow-100"
                      : "bg-red-100";
                  return (
                    <React.Fragment key={rowKey}>
                      <tr className={isInvalid ? "bg-red-50" : undefined}>
                        <td className="whitespace-nowrap px-4 py-3 text-sm text-gray-900">
                          {formatTimestamp(entry.ts)}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-sm text-gray-900">
                          {entry.actor || "—"}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-sm text-gray-900">
                          {entry.action || "—"}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-sm text-gray-900">
                          {entry.entity || "—"}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-sm">
                          <span
                            className={`inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${statusClass} ${statusBadgeClass}`.trim()}
                          >
                            {integrityLabel}
                          </span>
                        </td>
                      </tr>
                      {(entry.anomalies.length > 0 || entry.warnings.length > 0) && (
                        <tr className={isInvalid ? "bg-red-50" : "bg-yellow-50"}>
                          <td colSpan={5} className="px-4 pb-3 text-sm text-gray-700">
                            {entry.anomalies.length > 0 && (
                              <div className="mb-2">
                                <p className="font-semibold text-red-700">Anomalies</p>
                                <ul className="list-disc pl-5 text-red-700">
                                  {entry.anomalies.map((message, index) => (
                                    <li key={`${rowKey}-anomaly-${index}`}>{message}</li>
                                  ))}
                                </ul>
                              </div>
                            )}
                            {entry.warnings.length > 0 && (
                              <div>
                                <p className="font-semibold text-yellow-700">Warnings</p>
                                <ul className="list-disc pl-5 text-yellow-700">
                                  {entry.warnings.map((message, index) => (
                                    <li key={`${rowKey}-warning-${index}`}>{message}</li>
                                  ))}
                                </ul>
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
};

export default withErrorBoundary(AuditViewer, {
  componentName: "Audit Viewer",
});
