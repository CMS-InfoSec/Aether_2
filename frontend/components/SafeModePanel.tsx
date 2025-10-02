import React, { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useAuthClaims } from "./useAuthClaims";

type SafeModeStatusResponse = {
  active: boolean;
  reason?: string | null;
  since?: string | null;
  actor?: string | null;
};

type SafeModeAuditEntry = {
  reason?: string | null;
  timestamp?: string | null;
  actor?: string | null;
  state?: string | null;
};

const formatTimestamp = (timestamp?: string | null) => {
  if (!timestamp) {
    return "Unknown";
  }

  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }

  return date.toLocaleString();
};

const deriveEventLabel = (entry: SafeModeAuditEntry) => {
  const state = entry.state ? entry.state.toLowerCase() : "";
  if (state === "entered") {
    return "Entered";
  }
  if (state === "exited") {
    return "Exited";
  }
  return entry.state || "Unknown";
};

const SafeModePanel: React.FC = () => {
  const [status, setStatus] = useState<SafeModeStatusResponse | null>(null);
  const [statusLoading, setStatusLoading] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [auditTrail, setAuditTrail] = useState<SafeModeAuditEntry[]>([]);
  const [auditLoading, setAuditLoading] = useState(true);
  const [auditError, setAuditError] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [actor, setActor] = useState("");
  const [actionLoading, setActionLoading] = useState(false);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const { readOnly } = useAuthClaims();

  const currentStatusLabel = useMemo(() => {
    if (!status) {
      return "Loading";
    }
    return status.active ? "Active" : "Inactive";
  }, [status]);

  const fetchStatus = useCallback(
    async (signal: AbortSignal) => {
      try {
        setStatusLoading(true);
        const response = await fetch("/safe_mode/status", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load status (${response.status})`);
        }

        const payload = (await response.json()) as SafeModeStatusResponse & {
          history?: SafeModeAuditEntry[];
          events?: SafeModeAuditEntry[];
          audit?: SafeModeAuditEntry[];
        };

        setStatus(payload);
        setStatusError(null);

        const embeddedHistory =
          payload.history || payload.events || payload.audit;
        if (Array.isArray(embeddedHistory) && embeddedHistory.length > 0) {
          setAuditTrail(
            embeddedHistory.slice(-10).reverse()
          );
          setAuditError(null);
          setAuditLoading(false);
        }
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        console.error("Failed to fetch safe mode status", error);
        setStatusError("Unable to load safe mode status.");
      } finally {
        if (!signal.aborted) {
          setStatusLoading(false);
        }
      }
    },
    []
  );

  const fetchAuditTrail = useCallback(
    async (signal: AbortSignal) => {
      try {
        setAuditLoading(true);
        const response = await fetch("/safe_mode/log", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load audit trail (${response.status})`);
        }

        const payload = (await response.json()) as SafeModeAuditEntry[];
        setAuditTrail(payload.slice(-10).reverse());
        setAuditError(null);
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        console.error("Failed to fetch safe mode audit trail", error);
        setAuditError("Unable to load the safe mode audit trail.");
      } finally {
        if (!signal.aborted) {
          setAuditLoading(false);
        }
      }
    },
    []
  );

  const refreshAll = useCallback(async () => {
    const controller = new AbortController();

    try {
      await Promise.all([
        fetchStatus(controller.signal),
        fetchAuditTrail(controller.signal).catch((error) => {
          if (!controller.signal.aborted) {
            throw error;
          }
        }),
      ]);
    } finally {
      controller.abort();
    }
  }, [fetchStatus, fetchAuditTrail]);

  useEffect(() => {
    const controller = new AbortController();

    fetchStatus(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Safe mode status initialisation failed", error);
      }
    });

    fetchAuditTrail(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Safe mode audit trail initialisation failed", error);
      }
    });

    return () => {
      controller.abort();
    };
  }, [fetchStatus, fetchAuditTrail]);

  const handleEnterSafeMode = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (!reason.trim()) {
      setActionError("A reason is required to enter safe mode.");
      setActionMessage(null);
      return;
    }

    setActionLoading(true);
    setActionError(null);
    setActionMessage(null);

    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Accept: "application/json",
      };

      if (actor.trim()) {
        headers["X-Actor"] = actor.trim();
      }

      const response = await fetch("/safe_mode/enter", {
        method: "POST",
        headers,
        body: JSON.stringify({ reason: reason.trim() }),
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || "Failed to enter safe mode.");
      }

      setActionMessage("Safe mode engaged successfully.");
      setReason("");
      await refreshAll();
    } catch (error) {
      console.error("Failed to enter safe mode", error);
      setActionError(
        error instanceof Error ? error.message : "Unable to enter safe mode."
      );
    } finally {
      setActionLoading(false);
    }
  };

  const handleExitSafeMode = async () => {
    setActionLoading(true);
    setActionError(null);
    setActionMessage(null);

    try {
      const headers: Record<string, string> = {
        Accept: "application/json",
      };

      if (actor.trim()) {
        headers["X-Actor"] = actor.trim();
      }

      const response = await fetch("/safe_mode/exit", {
        method: "POST",
        headers,
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || "Failed to exit safe mode.");
      }

      setActionMessage("Safe mode exited successfully.");
      await refreshAll();
    } catch (error) {
      console.error("Failed to exit safe mode", error);
      setActionError(
        error instanceof Error ? error.message : "Unable to exit safe mode."
      );
    } finally {
      setActionLoading(false);
    }
  };

  return (
    <div className="bg-white shadow rounded-lg p-6 space-y-6">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div>
          <h2 className="text-2xl font-semibold text-gray-900">Safe Mode</h2>
          <p className="text-sm text-gray-500">
            Coordinate emergency trading controls and review audit history.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span
            className={`flex h-3 w-3 rounded-full ${
              status?.active ? "bg-red-500" : "bg-green-500"
            }`}
            aria-hidden="true"
          />
          <span className="text-sm font-medium text-gray-700">
            {statusLoading ? "Checking status..." : currentStatusLabel}
          </span>
        </div>
      </div>

      {!readOnly ? (
        <>
          <form
            onSubmit={handleEnterSafeMode}
            className="grid gap-4 md:grid-cols-2"
            aria-label="Safe mode controls"
          >
            <div className="md:col-span-2 grid gap-4 sm:grid-cols-2">
              <label className="flex flex-col text-sm font-medium text-gray-700">
                Reason
                <input
                  type="text"
                  value={reason}
                  onChange={(event) => setReason(event.target.value)}
                  placeholder="Describe the issue triggering safe mode"
                  className="mt-1 rounded-md border border-gray-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                  required
                  disabled={actionLoading}
                />
              </label>
              <label className="flex flex-col text-sm font-medium text-gray-700">
                Actor (optional)
                <input
                  type="text"
                  value={actor}
                  onChange={(event) => setActor(event.target.value)}
                  placeholder="ops-oncall"
                  className="mt-1 rounded-md border border-gray-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                  disabled={actionLoading}
                />
              </label>
            </div>

            <div className="flex flex-col gap-3 md:col-span-2 md:flex-row">
              <button
                type="submit"
                className="inline-flex items-center justify-center rounded-md bg-red-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-red-400"
                disabled={actionLoading}
              >
                {actionLoading ? "Processing..." : "Enter Safe Mode"}
              </button>
              <button
                type="button"
                onClick={handleExitSafeMode}
                className="inline-flex items-center justify-center rounded-md bg-green-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-green-400"
                disabled={actionLoading}
              >
                {actionLoading ? "Processing..." : "Exit Safe Mode"}
              </button>
              <button
                type="button"
                onClick={refreshAll}
                className="inline-flex items-center justify-center rounded-md border border-gray-300 px-4 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed"
                disabled={actionLoading}
              >
                Refresh
              </button>
            </div>
          </form>

          <div className="space-y-3">
            {actionMessage && (
              <div className="rounded-md bg-green-50 p-3 text-sm text-green-800">
                {actionMessage}
              </div>
            )}
            {actionError && (
              <div className="rounded-md bg-red-50 p-3 text-sm text-red-700">
                {actionError}
              </div>
            )}
          </div>
        </>
      ) : (
        <div className="space-y-3 md:col-span-2">
          <div className="rounded-md border border-gray-200 bg-gray-50 p-3 text-sm text-gray-600">
            Auditor access is read-only. Safe mode controls are hidden.
          </div>
          <button
            type="button"
            onClick={refreshAll}
            className="inline-flex items-center justify-center rounded-md border border-gray-300 px-4 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2"
          >
            Refresh
          </button>
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2">
        <div className="space-y-2">
          <h3 className="text-lg font-semibold text-gray-900">Current State</h3>
          {statusError && (
            <p className="text-sm text-red-600">{statusError}</p>
          )}
          <dl className="grid grid-cols-1 gap-x-4 gap-y-2 text-sm sm:grid-cols-2">
            <div>
              <dt className="font-medium text-gray-600">Status</dt>
              <dd className="text-gray-900">
                {statusLoading ? "Loading..." : currentStatusLabel}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-gray-600">Reason</dt>
              <dd className="text-gray-900">
                {status?.reason || (statusLoading ? "Loading..." : "Not set")}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-gray-600">Since</dt>
              <dd className="text-gray-900">
                {status?.since
                  ? formatTimestamp(status.since)
                  : statusLoading
                  ? "Loading..."
                  : "N/A"}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-gray-600">Actor</dt>
              <dd className="text-gray-900">
                {status?.actor || (statusLoading ? "Loading..." : "N/A")}
              </dd>
            </div>
          </dl>
        </div>
        <div className="space-y-2">
          <h3 className="text-lg font-semibold text-gray-900">Latest Activity</h3>
          {auditError && <p className="text-sm text-red-600">{auditError}</p>}
          <div className="max-h-64 overflow-y-auto rounded-md border border-gray-200">
            <ul className="divide-y divide-gray-200">
              {auditLoading ? (
                <li className="p-3 text-sm text-gray-500">Loading audit trail...</li>
              ) : auditTrail.length === 0 ? (
                <li className="p-3 text-sm text-gray-500">No audit events available.</li>
              ) : (
                auditTrail.map((entry, index) => (
                  <li key={`${entry.timestamp}-${index}`} className="p-3 text-sm">
                    <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
                      <span className="font-medium text-gray-900">
                        {deriveEventLabel(entry)}
                      </span>
                      <span className="text-xs text-gray-500">
                        {formatTimestamp(entry.timestamp)}
                      </span>
                    </div>
                    <p className="text-gray-700">
                      Reason: {entry.reason || "N/A"}
                    </p>
                    <p className="text-gray-500 text-xs">Actor: {entry.actor || "Unknown"}</p>
                  </li>
                ))
              )}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SafeModePanel;
