import React, { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { formatLondonTime } from "./timezone";
import { useAuthClaims } from "./useAuthClaims";

type SafeModeStatusResponse = {
  active: boolean;
  reason?: string | null;
  since?: string | null;
  actor?: string | null;
};

type SimulationAccountStatus = {
  account_id: string;
  active: boolean;
  reason?: string | null;
  since?: string | null;
  actor?: string | null;
};

type SimulationStatusEnvelope = {
  active: boolean;
  accounts: SimulationAccountStatus[];
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

  const formatted = formatLondonTime(timestamp);
  return formatted || "Unknown";
};

const SIMULATION_BADGE_ID = "simulation-mode-indicator";
const SIMULATION_BADGE_STYLE_ID = "simulation-mode-indicator-style";

const ensureSimulationBadgeElements = () => {
  if (typeof document === "undefined") {
    return null;
  }

  let styleElement = document.getElementById(
    SIMULATION_BADGE_STYLE_ID
  ) as HTMLStyleElement | null;

  if (!styleElement) {
    styleElement = document.createElement("style");
    styleElement.id = SIMULATION_BADGE_STYLE_ID;
    styleElement.textContent = `
      #${SIMULATION_BADGE_ID} {
        position: fixed;
        top: 16px;
        right: 16px;
        z-index: 9999;
        display: flex;
        flex-direction: column;
        gap: 4px;
        padding: 12px 16px;
        border-radius: 9999px;
        background: rgba(59, 130, 246, 0.95);
        color: #fff;
        box-shadow: 0 10px 30px rgba(59, 130, 246, 0.35);
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
          "Segoe UI", sans-serif;
      }

      #${SIMULATION_BADGE_ID}[data-hidden="true"] {
        display: none;
      }

      #${SIMULATION_BADGE_ID} .simulation-badge-label {
        font-weight: 600;
        font-size: 14px;
        display: flex;
        align-items: center;
        gap: 8px;
      }

      #${SIMULATION_BADGE_ID} .simulation-badge-dot {
        width: 10px;
        height: 10px;
        border-radius: 9999px;
        background: #22d3ee;
        box-shadow: 0 0 0 3px rgba(34, 211, 238, 0.35);
      }

      #${SIMULATION_BADGE_ID} .simulation-badge-detail {
        font-size: 12px;
        opacity: 0.9;
      }
    `;
    document.head.appendChild(styleElement);
  }

  let container = document.getElementById(
    SIMULATION_BADGE_ID
  ) as HTMLDivElement | null;

  if (!container) {
    container = document.createElement("div");
    container.id = SIMULATION_BADGE_ID;
    container.setAttribute("role", "status");
    container.setAttribute("aria-live", "polite");

    const label = document.createElement("div");
    label.className = "simulation-badge-label";

    const dot = document.createElement("span");
    dot.className = "simulation-badge-dot";
    label.appendChild(dot);

    const labelText = document.createElement("span");
    labelText.dataset.role = "label";
    label.appendChild(labelText);

    const detail = document.createElement("div");
    detail.className = "simulation-badge-detail";
    detail.dataset.role = "detail";

    const meta = document.createElement("div");
    meta.className = "simulation-badge-detail";
    meta.dataset.role = "meta";

    container.appendChild(label);
    container.appendChild(detail);
    container.appendChild(meta);

    document.body.appendChild(container);
  }

  return container;
};

const updateGlobalSimulationBadge = (status: SimulationStatusEnvelope | null) => {
  if (typeof document === "undefined") {
    return;
  }

  const container = document.getElementById(
    SIMULATION_BADGE_ID
  ) as HTMLDivElement | null;

  if (!status?.active) {
    if (container) {
      container.dataset.hidden = "true";
    }
    document.body.removeAttribute("data-simulation-mode");
    return;
  }

  const badge = ensureSimulationBadgeElements();
  if (!badge) {
    return;
  }

  badge.dataset.hidden = "false";
  document.body.setAttribute("data-simulation-mode", "active");

  const labelNode = badge.querySelector<HTMLSpanElement>('[data-role="label"]');
  const detailNode = badge.querySelector<HTMLDivElement>('[data-role="detail"]');
  const metaNode = badge.querySelector<HTMLDivElement>('[data-role="meta"]');

  const activeAccount =
    status.accounts.find((account) => account.active) || status.accounts[0];

  if (labelNode) {
    const accountLabel = activeAccount ? ` – ${activeAccount.account_id}` : "";
    labelNode.textContent = `Simulation Mode Active${accountLabel}`;
  }

  if (detailNode) {
    const reasonText = activeAccount?.reason?.trim();
    const actorText = activeAccount?.actor?.trim();
    if (reasonText) {
      detailNode.textContent = `Reason: ${reasonText}`;
    } else if (actorText) {
      detailNode.textContent = `Actor: ${actorText}`;
    } else if (activeAccount) {
      detailNode.textContent = `Account ${activeAccount.account_id} is running in simulation mode.`;
    } else {
      detailNode.textContent = "All trading activity is currently simulated.";
    }
    detailNode.style.display = "block";
  }

  if (metaNode) {
    if (activeAccount?.since) {
      metaNode.textContent = `Since ${formatTimestamp(activeAccount.since)}`;
      metaNode.style.display = "block";
    } else {
      metaNode.textContent = "";
      metaNode.style.display = "none";
    }
  }
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
  const [simulationOverview, setSimulationOverview] =
    useState<SimulationStatusEnvelope | null>(null);
  const [simulationLoading, setSimulationLoading] = useState(true);
  const [simulationError, setSimulationError] = useState<string | null>(null);
  const [simulationReason, setSimulationReason] = useState("");
  const [simulationActionLoading, setSimulationActionLoading] = useState(false);
  const [simulationMessage, setSimulationMessage] = useState<string | null>(null);
  const [simulationActionError, setSimulationActionError] =
    useState<string | null>(null);
  const [selectedSimulationAccount, setSelectedSimulationAccount] =
    useState<string>("");
  const { readOnly } = useAuthClaims();

  const simulationAccounts = useMemo(
    () => simulationOverview?.accounts ?? [],
    [simulationOverview]
  );

  const selectedSimulationStatus = useMemo(() => {
    if (simulationAccounts.length === 0) {
      return null;
    }
    if (!selectedSimulationAccount) {
      return simulationAccounts[0];
    }
    return (
      simulationAccounts.find(
        (entry) => entry.account_id === selectedSimulationAccount
      ) || simulationAccounts[0]
    );
  }, [simulationAccounts, selectedSimulationAccount]);

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

  const fetchSimulationStatus = useCallback(
    async (signal: AbortSignal) => {
      try {
        setSimulationLoading(true);
        const response = await fetch("/sim/status", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load simulation status (${response.status})`);
        }

        const payload = (await response.json()) as SimulationStatusEnvelope;
        setSimulationOverview(payload);
        setSimulationError(null);

        if (payload.accounts.length > 0) {
          const existing = payload.accounts.some(
            (entry) => entry.account_id === selectedSimulationAccount
          );
          if (!existing) {
            setSelectedSimulationAccount(payload.accounts[0].account_id);
          }
        }
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        console.error("Failed to fetch simulation status", error);
        setSimulationError("Unable to load simulation mode status.");
      } finally {
        if (!signal.aborted) {
          setSimulationLoading(false);
        }
      }
    },
    [selectedSimulationAccount]
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
        fetchSimulationStatus(controller.signal).catch((error) => {
          if (!controller.signal.aborted) {
            throw error;
          }
        }),
        fetchAuditTrail(controller.signal).catch((error) => {
          if (!controller.signal.aborted) {
            throw error;
          }
        }),
      ]);
    } finally {
      controller.abort();
    }
  }, [fetchStatus, fetchSimulationStatus, fetchAuditTrail]);

  useEffect(() => {
    const controller = new AbortController();

    fetchStatus(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Safe mode status initialisation failed", error);
      }
    });

    fetchSimulationStatus(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Simulation status initialisation failed", error);
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
  }, [fetchStatus, fetchSimulationStatus, fetchAuditTrail]);

  const handleEnterSafeMode = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (readOnly) {
      setActionError("Auditor access is read-only. Safe mode controls are disabled.");
      setActionMessage(null);
      return;
    }

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

  useEffect(() => {
    updateGlobalSimulationBadge(simulationOverview);
  }, [simulationOverview]);

  const handleExitSafeMode = async () => {
    if (readOnly) {
      setActionError("Auditor access is read-only. Safe mode controls are disabled.");
      setActionMessage(null);
      return;
    }
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

  const handleEnterSimulation = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (readOnly) {
      setSimulationActionError(
        "Auditor access is read-only. Simulation controls are disabled."
      );
      setSimulationMessage(null);
      return;
    }

    if (!selectedSimulationAccount) {
      setSimulationActionError("Select an account to control simulation mode.");
      setSimulationMessage(null);
      return;
    }

    if (!simulationReason.trim()) {
      setSimulationActionError("A reason is required to enter simulation mode.");
      setSimulationMessage(null);
      return;
    }

    setSimulationActionLoading(true);
    setSimulationActionError(null);
    setSimulationMessage(null);

    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Accept: "application/json",
      };

      if (actor.trim()) {
        headers["X-Actor"] = actor.trim();
      }

      const response = await fetch("/sim/enter", {
        method: "POST",
        headers,
        body: JSON.stringify({
          reason: simulationReason.trim(),
          account_id: selectedSimulationAccount,
        }),
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || "Failed to enter simulation mode.");
      }

      setSimulationMessage("Simulation mode engaged successfully.");
      setSimulationReason("");
      await refreshAll();
    } catch (error) {
      console.error("Failed to enter simulation mode", error);
      setSimulationActionError(
        error instanceof Error
          ? error.message
          : "Unable to enter simulation mode."
      );
    } finally {
      setSimulationActionLoading(false);
    }
  };

  const handleExitSimulation = async () => {
    if (readOnly) {
      setSimulationActionError(
        "Auditor access is read-only. Simulation controls are disabled."
      );
      setSimulationMessage(null);
      return;
    }

    if (!selectedSimulationAccount) {
      setSimulationActionError("Select an account to control simulation mode.");
      setSimulationMessage(null);
      return;
    }

    setSimulationActionLoading(true);
    setSimulationActionError(null);
    setSimulationMessage(null);

    try {
      const headers: Record<string, string> = {
        Accept: "application/json",
      };

      if (actor.trim()) {
        headers["X-Actor"] = actor.trim();
      }

      const params = new URLSearchParams({
        account_id: selectedSimulationAccount,
      });
      const response = await fetch(`/sim/exit?${params.toString()}`, {
        method: "POST",
        headers,
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || "Failed to exit simulation mode.");
      }

      setSimulationMessage("Simulation mode exited successfully.");
      await refreshAll();
    } catch (error) {
      console.error("Failed to exit simulation mode", error);
      setSimulationActionError(
        error instanceof Error
          ? error.message
          : "Unable to exit simulation mode."
      );
    } finally {
      setSimulationActionLoading(false);
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

      <>
        <form
          onSubmit={handleEnterSafeMode}
          className="grid gap-4 md:grid-cols-2"
          aria-label="Safe mode controls"
          aria-disabled={readOnly}
        >
          <fieldset disabled={readOnly} className="md:col-span-2 grid gap-4 sm:grid-cols-2">
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
              Actor (optional for all controls)
              <input
                type="text"
                value={actor}
                onChange={(event) => setActor(event.target.value)}
                placeholder="ops-oncall"
                className="mt-1 rounded-md border border-gray-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                disabled={actionLoading}
              />
            </label>
            <div className="flex flex-col gap-3 md:col-span-2 md:flex-row">
              <button
                type="submit"
                className="inline-flex items-center justify-center rounded-md bg-red-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-red-400"
                disabled={readOnly || actionLoading}
              >
                {actionLoading ? "Processing..." : "Enter Safe Mode"}
              </button>
              <button
                type="button"
                onClick={handleExitSafeMode}
                className="inline-flex items-center justify-center rounded-md bg-green-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-green-400"
                disabled={readOnly || actionLoading}
              >
                {actionLoading ? "Processing..." : "Exit Safe Mode"}
              </button>
              <button
                type="button"
                onClick={refreshAll}
                className="inline-flex items-center justify-center rounded-md border border-gray-300 px-4 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed"
                disabled={readOnly || actionLoading}
              >
                Refresh
              </button>
            </div>
          </fieldset>
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
          {readOnly && (
            <div className="rounded-md border border-gray-200 bg-gray-50 p-3 text-sm text-gray-600">
              Auditor access is read-only. Safe mode controls are disabled.
            </div>
          )}
        </div>
      </>

      <>
        <form
          onSubmit={handleEnterSimulation}
          className="grid gap-4 md:grid-cols-2"
          aria-label="Simulation mode controls"
          aria-disabled={readOnly}
        >
          <fieldset
            disabled={readOnly}
            className="md:col-span-2 grid gap-4 sm:grid-cols-2"
          >
            <label className="flex flex-col text-sm font-medium text-gray-700">
              Simulation Account
              <select
                value={selectedSimulationAccount}
                onChange={(event) =>
                  setSelectedSimulationAccount(event.target.value)
                }
                className="mt-1 rounded-md border border-gray-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                disabled={simulationActionLoading || simulationAccounts.length === 0}
                required
              >
                {simulationAccounts.length === 0 ? (
                  <option value="">No accounts available</option>
                ) : (
                  simulationAccounts.map((entry) => (
                    <option key={entry.account_id} value={entry.account_id}>
                      {entry.account_id}
                    </option>
                  ))
                )}
              </select>
            </label>
            <label className="flex flex-col text-sm font-medium text-gray-700">
              Simulation Reason
              <input
                type="text"
                value={simulationReason}
                onChange={(event) => setSimulationReason(event.target.value)}
                placeholder="Describe the purpose for simulation mode"
                className="mt-1 rounded-md border border-gray-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                required
                disabled={simulationActionLoading}
              />
            </label>
            <div className="flex flex-col gap-3 md:col-span-2 md:flex-row">
              <button
                type="submit"
                className="inline-flex items-center justify-center rounded-md bg-blue-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-blue-400"
                disabled={readOnly || simulationActionLoading}
              >
                {simulationActionLoading ? "Processing..." : "Enter Simulation"}
              </button>
              <button
                type="button"
                onClick={handleExitSimulation}
                className="inline-flex items-center justify-center rounded-md bg-slate-700 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-slate-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-slate-400"
                disabled={readOnly || simulationActionLoading}
              >
                {simulationActionLoading ? "Processing..." : "Exit Simulation"}
              </button>
              <button
                type="button"
                onClick={refreshAll}
                className="inline-flex items-center justify-center rounded-md border border-gray-300 px-4 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed"
                disabled={readOnly || simulationActionLoading}
              >
                Refresh
              </button>
            </div>
          </fieldset>
        </form>

        <div className="space-y-3">
          {simulationMessage && (
            <div className="rounded-md bg-blue-50 p-3 text-sm text-blue-800">
              {simulationMessage}
            </div>
          )}
          {simulationActionError && (
            <div className="rounded-md bg-red-50 p-3 text-sm text-red-700">
              {simulationActionError}
            </div>
          )}
          {simulationError && (
            <div className="rounded-md bg-yellow-50 p-3 text-sm text-yellow-700">
              {simulationError}
            </div>
          )}
        </div>
      </>

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
            <div>
              <dt className="font-medium text-gray-600">Simulation Status</dt>
              <dd className="text-gray-900">
                {simulationLoading
                  ? "Loading..."
                  : selectedSimulationStatus?.active
                  ? "Active"
                  : "Inactive"}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-gray-600">Simulation Reason</dt>
              <dd className="text-gray-900">
                {selectedSimulationStatus?.reason ||
                  (simulationLoading ? "Loading..." : "Not set")}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-gray-600">Simulation Since</dt>
              <dd className="text-gray-900">
                {selectedSimulationStatus?.since
                  ? formatTimestamp(selectedSimulationStatus.since)
                  : simulationLoading
                  ? "Loading..."
                  : "N/A"}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-gray-600">Simulation Actor</dt>
              <dd className="text-gray-900">
                {selectedSimulationStatus?.actor ||
                  (simulationLoading ? "Loading..." : "N/A")}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-gray-600">Simulation Account</dt>
              <dd className="text-gray-900">
                {simulationLoading
                  ? "Loading..."
                  : selectedSimulationStatus?.account_id || "N/A"}
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
