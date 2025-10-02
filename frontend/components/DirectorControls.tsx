import React, {
  FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { useAuthClaims } from "./useAuthClaims";

interface SafeModeStatusResponse {
  active: boolean;
  reason?: string | null;
  since?: string | null;
  actor?: string | null;
}

interface SafeModeActionPayload {
  reason?: string;
  actor?: string;
}

interface KillSwitchResponse {
  status?: string;
  ts?: string;
  reason_code?: string;
}

type KillSwitchReason = "loss_cap_breach" | "spread_widening" | "latency_stall";

type TradingPair = string;

interface DirectorAction {
  id?: string;
  action: string;
  actor: string;
  entity: string;
  timestamp: string;
  details?: string;
}

interface OverridePayload {
  symbol: string;
  enabled: boolean;
  reason?: string;
  actor?: string;
}

const KILL_SWITCH_REASONS: { label: string; value: KillSwitchReason }[] = [
  { label: "Daily Loss Cap Breach", value: "loss_cap_breach" },
  { label: "Spread Widening", value: "spread_widening" },
  { label: "Latency Stall", value: "latency_stall" },
];

const DIRECTOR_ACCOUNTS = new Set(["director-1", "director-2", "company"]);

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

const ensureString = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  return String(value);
};

const mapAuditEntry = (raw: unknown): DirectorAction | null => {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const record = raw as Record<string, unknown>;

  const actor = ensureString(
    record.actor ?? record.actor_id ?? record.user ?? record.principal ?? ""
  );
  if (!DIRECTOR_ACCOUNTS.has(actor)) {
    return null;
  }

  const action = ensureString(record.action ?? record.event ?? "");
  const entity = ensureString(
    record.entity ??
      record.entity_id ??
      record.target ??
      record.resource ??
      record.subject ??
      record.object ??
      ""
  );
  const timestamp = ensureString(
    record.ts ?? record.timestamp ?? record.created_at ?? record.event_time ?? ""
  );

  if (!action || !timestamp) {
    return null;
  }

  const idValue = record.id ?? record.event_id ?? record.audit_id ?? null;
  const id =
    idValue === null || idValue === undefined ? undefined : ensureString(idValue);

  const detailsValue = record.details ?? record.after ?? record.metadata ?? null;
  const details =
    detailsValue && typeof detailsValue === "object"
      ? JSON.stringify(detailsValue)
      : ensureString(detailsValue);

  return {
    id,
    action,
    actor,
    entity: entity || "—",
    timestamp,
    details: details || undefined,
  };
};

const extractDirectorActions = (payload: unknown): DirectorAction[] => {
  const tryExtractArray = (value: unknown): unknown[] => {
    if (Array.isArray(value)) {
      return value;
    }
    if (value && typeof value === "object") {
      const container = value as Record<string, unknown>;
      const candidateKeys = ["logs", "results", "data", "entries"] as const;
      for (const key of candidateKeys) {
        const candidate = container[key];
        if (Array.isArray(candidate)) {
          return candidate as unknown[];
        }
      }
    }
    return [];
  };

  return tryExtractArray(payload)
    .map((item) => mapAuditEntry(item))
    .filter((entry): entry is DirectorAction => entry !== null)
    .sort((first, second) => {
      const a = Date.parse(first.timestamp);
      const b = Date.parse(second.timestamp);
      if (!Number.isNaN(a) && !Number.isNaN(b)) {
        return b - a;
      }
      return second.timestamp.localeCompare(first.timestamp);
    })
    .slice(0, 20);
};

const DirectorControls: React.FC = () => {
  const [safeModeStatus, setSafeModeStatus] =
    useState<SafeModeStatusResponse | null>(null);
  const [safeModeLoading, setSafeModeLoading] = useState<boolean>(true);
  const [safeModeError, setSafeModeError] = useState<string | null>(null);
  const [safeModeReason, setSafeModeReason] = useState<string>("");
  const [safeModeActor, setSafeModeActor] = useState<string>("");
  const [safeModeMessage, setSafeModeMessage] = useState<string | null>(null);
  const [safeModeActionError, setSafeModeActionError] = useState<string | null>(
    null
  );
  const [safeModeActionLoading, setSafeModeActionLoading] =
    useState<boolean>(false);

  const [killAccount, setKillAccount] = useState<string>("ACC-PRIMARY");
  const [killReason, setKillReason] =
    useState<KillSwitchReason>("loss_cap_breach");
  const [firstApproval, setFirstApproval] = useState<string>("");
  const [secondApproval, setSecondApproval] = useState<string>("");
  const [initiatingAccount, setInitiatingAccount] = useState<string>("director-1");
  const [killLoading, setKillLoading] = useState<boolean>(false);
  const [killMessage, setKillMessage] = useState<string | null>(null);
  const [killError, setKillError] = useState<string | null>(null);
  const [isKillModalOpen, setIsKillModalOpen] = useState<boolean>(false);
  const [killConfirmationText, setKillConfirmationText] = useState<string>("");

  const [availablePairs, setAvailablePairs] = useState<TradingPair[]>([]);
  const [overridePair, setOverridePair] = useState<TradingPair>("");
  const [overrideEnabled, setOverrideEnabled] = useState<boolean>(false);
  const [overrideReason, setOverrideReason] = useState<string>("");
  const [overrideActor, setOverrideActor] = useState<string>("");
  const [overrideLoading, setOverrideLoading] = useState<boolean>(false);
  const [overrideMessage, setOverrideMessage] = useState<string | null>(null);
  const [overrideError, setOverrideError] = useState<string | null>(null);

  const [auditTrail, setAuditTrail] = useState<DirectorAction[]>([]);
  const [auditLoading, setAuditLoading] = useState<boolean>(true);
  const [auditError, setAuditError] = useState<string | null>(null);
  const { readOnly } = useAuthClaims();

  const safeModeStatusLabel = useMemo(() => {
    if (!safeModeStatus) {
      return "Unknown";
    }
    return safeModeStatus.active ? "Active" : "Inactive";
  }, [safeModeStatus]);

  const loadSafeModeStatus = useCallback(
    async (signal: AbortSignal) => {
      try {
        setSafeModeLoading(true);
        const response = await fetch("/safe_mode/status", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load safe mode status (${response.status})`);
        }

        const payload = (await response.json()) as SafeModeStatusResponse;
        setSafeModeStatus(payload);
        setSafeModeError(null);
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        console.error("Failed to fetch safe mode status", error);
        setSafeModeError("Unable to load safe mode status.");
      } finally {
        if (!signal.aborted) {
          setSafeModeLoading(false);
        }
      }
    },
    []
  );

  const loadAuditTrail = useCallback(
    async (signal: AbortSignal) => {
      try {
        setAuditLoading(true);
        const response = await fetch("/audit/query", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load director actions (${response.status})`);
        }

        const data = await response.json();
        setAuditTrail(extractDirectorActions(data));
        setAuditError(null);
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        console.error("Failed to fetch director action audit trail", error);
        setAuditError("Unable to load director action history.");
      } finally {
        if (!signal.aborted) {
          setAuditLoading(false);
        }
      }
    },
    []
  );

  const loadTradingPairs = useCallback(
    async (signal: AbortSignal) => {
      try {
        const response = await fetch("/universe/approved", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load trading pairs (${response.status})`);
        }

        const payload = (await response.json()) as { symbols?: TradingPair[] };
        const symbols = Array.isArray(payload.symbols) ? payload.symbols : [];
        setAvailablePairs(symbols);
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        console.error("Failed to fetch trading pairs", error);
      }
    },
    []
  );

  const refreshAuditTrail = useCallback(async () => {
    const controller = new AbortController();
    try {
      await loadAuditTrail(controller.signal);
    } finally {
      controller.abort();
    }
  }, [loadAuditTrail]);

  const refreshSafeModeStatus = useCallback(async () => {
    const controller = new AbortController();
    try {
      await loadSafeModeStatus(controller.signal);
    } finally {
      controller.abort();
    }
  }, [loadSafeModeStatus]);

  useEffect(() => {
    const controller = new AbortController();

    loadSafeModeStatus(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Failed to initialize safe mode status", error);
      }
    });

    return () => {
      controller.abort();
    };
  }, [loadSafeModeStatus]);

  useEffect(() => {
    const controller = new AbortController();

    loadAuditTrail(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Failed to initialize director action audit", error);
      }
    });

    return () => {
      controller.abort();
    };
  }, [loadAuditTrail]);

  useEffect(() => {
    const controller = new AbortController();

    loadTradingPairs(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Failed to initialize trading pairs", error);
      }
    });

    return () => {
      controller.abort();
    };
  }, [loadTradingPairs]);

  const handleSafeMode = async (action: "enter" | "exit") => {
    if (readOnly) {
      setSafeModeActionError("Auditor access is read-only. Safe mode controls are disabled.");
      setSafeModeMessage(null);
      return;
    }
    setSafeModeActionError(null);
    setSafeModeMessage(null);

    if (action === "enter" && safeModeReason.trim().length === 0) {
      setSafeModeActionError(
        "Please provide a reason before entering safe mode."
      );
      return;
    }

    const payload: SafeModeActionPayload = {
      reason: safeModeReason.trim() || undefined,
      actor: safeModeActor.trim() || undefined,
    };

    setSafeModeActionLoading(true);

    try {
      const response = await fetch(`/safe_mode/${action}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          ...(safeModeActor.trim() ? { "X-Actor": safeModeActor.trim() } : {}),
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error(
          `Failed to ${action === "enter" ? "enter" : "exit"} safe mode (${response.status})`
        );
      }

      setSafeModeMessage(
        action === "enter"
          ? "Safe mode engaged successfully."
          : "Safe mode exited successfully."
      );
      if (action === "enter") {
        setSafeModeReason("");
      }
      await refreshSafeModeStatus();
      await refreshAuditTrail();
    } catch (error) {
      console.error("Safe mode action failed", error);
      setSafeModeActionError("Unable to update safe mode state. Please try again.");
    } finally {
      setSafeModeActionLoading(false);
    }
  };

  const validateKillSwitchForm = () => {
    const approvals = [firstApproval.trim(), secondApproval.trim()].filter(
      (value) => value.length > 0
    );

    if (approvals.length < 2) {
      setKillError("Two distinct director approvals are required.");
      return false;
    }

    if (approvals[0].toLowerCase() === approvals[1].toLowerCase()) {
      setKillError("Director approvals must belong to different directors.");
      return false;
    }

    if (killAccount.trim().length === 0) {
      setKillError("Please specify an account identifier.");
      return false;
    }

    if (!initiatingAccount.trim()) {
      setKillError("Select an initiating administrator account.");
      return false;
    }

    setKillError(null);
    return true;
  };

  const handleKillSwitchRequest = async () => {
    if (readOnly) {
      setKillError("Auditor access is read-only. Kill switch controls are disabled.");
      setKillMessage(null);
      return;
    }
    const approvals = [firstApproval.trim(), secondApproval.trim()].filter(
      (value) => value.length > 0
    );

    setKillLoading(true);

    try {
      const params = new URLSearchParams({
        account_id: killAccount.trim(),
        reason_code: killReason,
      });

      const response = await fetch(`/risk/kill?${params.toString()}`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "X-Director-Approvals": approvals.join(","),
          "X-Account-ID": initiatingAccount.trim(),
        },
      });

      if (!response.ok) {
        throw new Error(`Kill switch request failed (${response.status})`);
      }

      const payload = (await response.json()) as KillSwitchResponse;
      setKillMessage(
        `Kill switch engaged${
          payload.ts ? ` at ${formatTimestamp(payload.ts)}` : ""
        }${payload.reason_code ? ` • ${payload.reason_code}` : ""}`
      );
      setKillConfirmationText("");
      setIsKillModalOpen(false);
      await refreshAuditTrail();
      await refreshSafeModeStatus();
    } catch (error) {
      console.error("Kill switch request failed", error);
      setKillError("Unable to execute the kill switch. Please try again.");
    } finally {
      setKillLoading(false);
    }
  };

  const handleKillSwitch = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (readOnly) {
      setKillError("Auditor access is read-only. Kill switch controls are disabled.");
      setKillMessage(null);
      return;
    }
    setKillMessage(null);

    if (!validateKillSwitchForm()) {
      return;
    }

    setIsKillModalOpen(true);
  };

  const handleOverride = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (readOnly) {
      setOverrideError("Auditor access is read-only. Override controls are disabled.");
      setOverrideMessage(null);
      return;
    }

    setOverrideError(null);
    setOverrideMessage(null);

    if (!overridePair.trim()) {
      setOverrideError("Select a trading pair to override.");
      return;
    }

    if (!overrideReason.trim()) {
      setOverrideError("Please provide a reason for the override decision.");
      return;
    }

    setOverrideLoading(true);

    const payload: OverridePayload = {
      symbol: overridePair.trim().toUpperCase(),
      enabled: overrideEnabled,
      reason: overrideReason.trim(),
      actor: overrideActor.trim() || undefined,
    };

    try {
      const response = await fetch("/universe/override", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error(`Failed to submit override (${response.status})`);
      }

      setOverrideMessage(
        `Override submitted: ${payload.symbol} ${
          payload.enabled ? "enabled" : "disabled"
        }.`
      );
      setOverrideReason("");
      setOverrideActor("");
      await refreshAuditTrail();
    } catch (error) {
      console.error("Override submission failed", error);
      setOverrideError(
        "Unable to submit the override decision. Please try again later."
      );
    } finally {
      setOverrideLoading(false);
    }
  };

  return (
    <section className="max-w-6xl mx-auto px-4 py-8 space-y-8">
      <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 className="text-2xl font-semibold text-slate-900">
            Director Controls
          </h2>
          <p className="text-sm text-slate-500">
            Manage emergency actions, trading overrides, and review the latest
            director activity.
          </p>
        </div>
        <div className="rounded-lg border border-slate-200 px-4 py-2 text-sm text-slate-600">
          <span className="font-medium text-slate-900">Safe Mode:</span>{" "}
          {safeModeLoading ? "Checking…" : safeModeStatusLabel}
        </div>
      </header>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="space-y-6">
          <div className="rounded-lg bg-white p-6 shadow-sm ring-1 ring-slate-200">
            <div className="flex items-start justify-between">
              <h3 className="text-lg font-semibold text-slate-900">Safe Mode</h3>
              {safeModeStatus?.active && (
                <span className="rounded-full bg-amber-100 px-3 py-1 text-xs font-medium text-amber-800">
                  Active
                </span>
              )}
            </div>
            <p className="mt-2 text-sm text-slate-600">
              Current status: <strong>{safeModeStatusLabel}</strong>
              {safeModeStatus?.since && (
                <span className="ml-1 text-slate-500">
                  since {formatTimestamp(safeModeStatus.since)}
                </span>
              )}
              {safeModeStatus?.reason && (
                <span className="ml-1 text-slate-500">
                  • Reason: <em>{safeModeStatus.reason}</em>
                </span>
              )}
            </p>
            {safeModeLoading && (
              <p className="mt-3 text-sm text-slate-500">
                Loading safe mode status…
              </p>
            )}
            {safeModeError && (
              <p className="mt-3 text-sm text-rose-600">{safeModeError}</p>
            )}
            <div className="mt-4 space-y-4" aria-disabled={readOnly}>
              <fieldset disabled={readOnly} className="space-y-4">
                <div className="grid gap-3 sm:grid-cols-2">
                  <label className="text-sm font-medium text-slate-700" htmlFor="safe-mode-reason">
                    Reason
                  </label>
                  <input
                    id="safe-mode-reason"
                    type="text"
                    value={safeModeReason}
                    onChange={(event) => setSafeModeReason(event.target.value)}
                    placeholder="Reason for entering safe mode"
                    className="sm:col-span-1 rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                  />
                  <label className="text-sm font-medium text-slate-700" htmlFor="safe-mode-actor">
                    Actor (optional)
                  </label>
                  <input
                    id="safe-mode-actor"
                    type="text"
                    value={safeModeActor}
                    onChange={(event) => setSafeModeActor(event.target.value)}
                    placeholder="Recorded actor"
                    className="sm:col-span-1 rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                  />
                </div>
                <div className="flex flex-col gap-3 sm:flex-row">
                  <button
                    type="button"
                    onClick={() => handleSafeMode("enter")}
                    disabled={readOnly || safeModeActionLoading}
                    className="inline-flex items-center justify-center rounded-md bg-amber-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-amber-500 disabled:cursor-not-allowed disabled:bg-amber-300"
                  >
                    Enter Safe Mode
                  </button>
                  <button
                    type="button"
                    onClick={() => handleSafeMode("exit")}
                    disabled={readOnly || safeModeActionLoading}
                    className="inline-flex items-center justify-center rounded-md bg-emerald-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-emerald-500 disabled:cursor-not-allowed disabled:bg-emerald-300"
                  >
                    Exit Safe Mode
                  </button>
                </div>
              </fieldset>
              {safeModeMessage && (
                <p className="text-sm text-emerald-600">{safeModeMessage}</p>
              )}
              {safeModeActionError && (
                <p className="text-sm text-rose-600">{safeModeActionError}</p>
              )}
            </div>
            {readOnly && (
              <p className="mt-4 rounded-md bg-slate-50 p-3 text-sm text-slate-600">
                Auditor access is read-only. Safe mode controls are disabled.
              </p>
            )}
          </div>

          <div className="rounded-lg bg-white p-6 shadow-sm ring-1 ring-slate-200">
            <h3 className="text-lg font-semibold text-slate-900">Kill Switch</h3>
            <>
              <form onSubmit={handleKillSwitch} className="mt-4 space-y-4" aria-disabled={readOnly}>
                <fieldset disabled={readOnly} className="space-y-4">
                  <div className="grid gap-4 sm:grid-cols-2">
                    <div className="space-y-1">
                      <label
                        htmlFor="kill-account"
                        className="text-sm font-medium text-slate-700"
                      >
                        Account
                      </label>
                      <input
                        id="kill-account"
                        type="text"
                        value={killAccount}
                        onChange={(event) => setKillAccount(event.target.value)}
                        placeholder="Account identifier"
                        className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      />
                    </div>
                    <div className="space-y-1">
                      <label
                        htmlFor="kill-reason"
                        className="text-sm font-medium text-slate-700"
                      >
                        Reason
                      </label>
                      <select
                        id="kill-reason"
                        value={killReason}
                        onChange={(event) =>
                          setKillReason(event.target.value as KillSwitchReason)
                        }
                        className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      >
                        {KILL_SWITCH_REASONS.map((reason) => (
                          <option key={reason.value} value={reason.value}>
                            {reason.label}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div className="space-y-1">
                      <label
                        htmlFor="first-approval"
                        className="text-sm font-medium text-slate-700"
                      >
                        Director Approval #1
                      </label>
                      <input
                        id="first-approval"
                        type="text"
                        value={firstApproval}
                        onChange={(event) => setFirstApproval(event.target.value)}
                        placeholder="director-1"
                        className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      />
                    </div>
                    <div className="space-y-1">
                      <label
                        htmlFor="second-approval"
                        className="text-sm font-medium text-slate-700"
                      >
                        Director Approval #2
                      </label>
                      <input
                        id="second-approval"
                        type="text"
                        value={secondApproval}
                        onChange={(event) => setSecondApproval(event.target.value)}
                        placeholder="director-2"
                        className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      />
                    </div>
                    <div className="space-y-1">
                      <label
                        htmlFor="initiating-account"
                        className="text-sm font-medium text-slate-700"
                      >
                        Initiating Account
                      </label>
                      <input
                        id="initiating-account"
                        type="text"
                        value={initiatingAccount}
                        onChange={(event) =>
                          setInitiatingAccount(event.target.value)
                        }
                        placeholder="director-1"
                        className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      />
                    </div>
                  </div>
                  <button
                    type="submit"
                    className="inline-flex w-full items-center justify-center rounded-md bg-rose-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-rose-500 disabled:cursor-not-allowed disabled:bg-rose-300"
                    disabled={readOnly || killLoading}
                  >
                    Engage Kill Switch
                  </button>
                </fieldset>
              </form>
              {killMessage && (
                <p className="mt-3 text-sm text-amber-600">{killMessage}</p>
              )}
              {killError && (
                <p className="mt-3 text-sm text-rose-600">{killError}</p>
              )}
              {readOnly && (
                <p className="mt-4 rounded-md bg-slate-50 p-3 text-sm text-slate-600">
                  Auditor access is read-only. Kill switch controls are disabled.
                </p>
              )}
            </>
          </div>
        </div>

        <div className="space-y-6">
          <div className="rounded-lg bg-white p-6 shadow-sm ring-1 ring-slate-200">
            <h3 className="text-lg font-semibold text-slate-900">
              Trading Pair Overrides
            </h3>
            <p className="mt-2 text-sm text-slate-600">
              Temporarily enable or disable manual overrides for specific USD
              trading pairs.
            </p>
            <>
              <form onSubmit={handleOverride} className="mt-4 space-y-4" aria-disabled={readOnly}>
                <fieldset disabled={readOnly} className="space-y-4">
                  <div className="space-y-1">
                    <label
                      htmlFor="override-pair"
                      className="text-sm font-medium text-slate-700"
                    >
                      Trading Pair
                    </label>
                    <select
                      id="override-pair"
                      value={overridePair}
                      onChange={(event) => setOverridePair(event.target.value)}
                      className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    >
                      <option value="">Select a trading pair…</option>
                      {availablePairs.map((pair) => (
                        <option key={pair} value={pair}>
                          {pair}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="space-y-1">
                    <span className="text-sm font-medium text-slate-700">
                      Override Action
                    </span>
                    <div className="flex flex-col gap-2 sm:flex-row">
                      <label className="flex items-center gap-2 text-sm text-slate-700">
                        <input
                          type="radio"
                          name="override-enabled"
                          value="disable"
                          checked={!overrideEnabled}
                          onChange={() => setOverrideEnabled(false)}
                          className="h-4 w-4 border-slate-300 text-rose-600 focus:ring-rose-500"
                        />
                        Disable trading pair
                      </label>
                      <label className="flex items-center gap-2 text-sm text-slate-700">
                        <input
                          type="radio"
                          name="override-enabled"
                          value="enable"
                          checked={overrideEnabled}
                          onChange={() => setOverrideEnabled(true)}
                          className="h-4 w-4 border-slate-300 text-emerald-600 focus:ring-emerald-500"
                        />
                        Enable trading pair
                      </label>
                    </div>
                  </div>
                  <div className="space-y-1">
                    <label
                      htmlFor="override-reason"
                      className="text-sm font-medium text-slate-700"
                    >
                      Reason
                    </label>
                    <input
                      id="override-reason"
                      type="text"
                      value={overrideReason}
                      onChange={(event) => setOverrideReason(event.target.value)}
                      placeholder="Why this override is needed"
                      className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    />
                  </div>
                  <div className="space-y-1">
                    <label
                      htmlFor="override-actor"
                      className="text-sm font-medium text-slate-700"
                    >
                      Actor (optional)
                    </label>
                    <input
                      id="override-actor"
                      type="text"
                      value={overrideActor}
                      onChange={(event) => setOverrideActor(event.target.value)}
                      placeholder="ops_team"
                      className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    />
                  </div>
                  <button
                    type="submit"
                    disabled={readOnly || overrideLoading}
                    className="inline-flex w-full items-center justify-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-indigo-500 disabled:cursor-not-allowed disabled:bg-indigo-300"
                  >
                    Submit Override
                  </button>
                </fieldset>
              </form>
              {overrideMessage && (
                <p className="mt-3 text-sm text-emerald-600">{overrideMessage}</p>
              )}
              {overrideError && (
                <p className="mt-3 text-sm text-rose-600">{overrideError}</p>
              )}
              {readOnly && (
                <p className="mt-4 rounded-md bg-slate-50 p-3 text-sm text-slate-600">
                  Auditor access is read-only. Override controls are disabled.
                </p>
              )}
            </>
          </div>

          <div className="rounded-lg bg-white p-6 shadow-sm ring-1 ring-slate-200">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-slate-900">
                Recent Director Actions
              </h3>
              <button
                type="button"
                onClick={refreshAuditTrail}
                className="text-sm font-medium text-indigo-600 hover:text-indigo-500"
              >
                Refresh
              </button>
            </div>
            {auditLoading && (
              <p className="mt-3 text-sm text-slate-500">
                Loading director actions…
              </p>
            )}
            {auditError && (
              <p className="mt-3 text-sm text-rose-600">{auditError}</p>
            )}
            {!auditLoading && !auditError && auditTrail.length === 0 && (
              <p className="mt-3 text-sm text-slate-600">
                No director actions recorded.
              </p>
            )}
            {!auditLoading && !auditError && auditTrail.length > 0 && (
              <div className="mt-4 overflow-hidden rounded-lg border border-slate-200">
                <table className="min-w-full divide-y divide-slate-200 text-sm">
                  <thead className="bg-slate-50 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
                    <tr>
                      <th className="px-4 py-2">Timestamp</th>
                      <th className="px-4 py-2">Action</th>
                      <th className="px-4 py-2">Actor</th>
                      <th className="px-4 py-2">Entity</th>
                      <th className="px-4 py-2">Details</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200 bg-white">
                    {auditTrail.map((entry, index) => (
                      <tr key={entry.id ?? `${entry.action}-${index}`}>
                        <td className="px-4 py-2 text-slate-700">
                          {formatTimestamp(entry.timestamp)}
                        </td>
                        <td className="px-4 py-2 text-slate-700">{entry.action}</td>
                        <td className="px-4 py-2 text-slate-700">
                          {entry.actor || "Unknown"}
                        </td>
                        <td className="px-4 py-2 text-slate-700">{entry.entity}</td>
                        <td className="px-4 py-2 text-slate-500">
                          {entry.details ?? ""}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>

      {isKillModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 px-4">
          <div className="w-full max-w-lg rounded-lg bg-white p-6 shadow-xl">
            <h4 className="text-lg font-semibold text-slate-900">Confirm Kill Switch</h4>
            <p className="mt-2 text-sm text-slate-600">
              You are about to broadcast a kill switch for account
              <span className="font-semibold"> {killAccount.trim()}</span>. This
              action will cancel all open orders and halt trading. Enter
              <code className="mx-1 rounded bg-slate-100 px-1 py-0.5 text-xs text-slate-700">
                CONFIRM
              </code>{" "}
              to proceed.
            </p>
            <div className="mt-4 space-y-4">
              <div className="grid gap-2 rounded-md bg-slate-50 p-3 text-sm text-slate-600">
                <div>
                  <span className="font-medium text-slate-700">Reason:</span>{" "}
                  {KILL_SWITCH_REASONS.find((item) => item.value === killReason)?.label}
                </div>
                <div>
                  <span className="font-medium text-slate-700">Approvals:</span>{" "}
                  {[firstApproval.trim(), secondApproval.trim()].join(", ")}
                </div>
                <div>
                  <span className="font-medium text-slate-700">Initiating Account:</span>{" "}
                  {initiatingAccount.trim()}
                </div>
              </div>
              <input
                type="text"
                value={killConfirmationText}
                onChange={(event) => setKillConfirmationText(event.target.value)}
                placeholder="Type CONFIRM to continue"
                className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-rose-500 focus:outline-none focus:ring-2 focus:ring-rose-500"
              />
            </div>
            <div className="mt-6 flex flex-col gap-3 sm:flex-row sm:justify-end">
              <button
                type="button"
                onClick={() => {
                  setIsKillModalOpen(false);
                  setKillConfirmationText("");
                }}
                className="inline-flex items-center justify-center rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 shadow-sm transition hover:bg-slate-50"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={killConfirmationText.trim().toUpperCase() !== "CONFIRM" || killLoading}
                onClick={handleKillSwitchRequest}
                className="inline-flex items-center justify-center rounded-md bg-rose-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-rose-500 disabled:cursor-not-allowed disabled:bg-rose-300"
              >
                Confirm Kill Switch
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
};

export default DirectorControls;
