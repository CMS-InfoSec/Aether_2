import React, {
  FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

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

type DirectorAction = {
  id?: string | number;
  action: string;
  actor?: string | null;
  target?: string | null;
  details?: string | null;
  timestamp?: string | null;
};

interface AssetOverridePayload {
  asset: string;
  reason: string;
  action: "block" | "unblock";
}

const KILL_SWITCH_REASONS: { label: string; value: KillSwitchReason }[] = [
  { label: "Daily Loss Cap Breach", value: "loss_cap_breach" },
  { label: "Spread Widening", value: "spread_widening" },
  { label: "Latency Stall", value: "latency_stall" },
];

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
  const [killArmed, setKillArmed] = useState<boolean>(false);
  const [killLoading, setKillLoading] = useState<boolean>(false);
  const [killMessage, setKillMessage] = useState<string | null>(null);
  const [killError, setKillError] = useState<string | null>(null);

  const [overrideAsset, setOverrideAsset] = useState<string>("");
  const [overrideReason, setOverrideReason] = useState<string>("");
  const [overrideAction, setOverrideAction] = useState<"block" | "unblock">(
    "block"
  );
  const [overrideLoading, setOverrideLoading] = useState<boolean>(false);
  const [overrideMessage, setOverrideMessage] = useState<string | null>(null);
  const [overrideError, setOverrideError] = useState<string | null>(null);

  const [auditTrail, setAuditTrail] = useState<DirectorAction[]>([]);
  const [auditLoading, setAuditLoading] = useState<boolean>(true);
  const [auditError, setAuditError] = useState<string | null>(null);

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
        const response = await fetch("/director/actions?limit=20", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load director actions (${response.status})`);
        }

        const data = (await response.json()) as DirectorAction[];
        setAuditTrail(Array.isArray(data) ? data.slice(0, 20) : []);
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

  const handleSafeMode = async (action: "enter" | "exit") => {
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

  const handleKillSwitch = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    setKillError(null);
    setKillMessage(null);

    const approvals = [firstApproval.trim(), secondApproval.trim()].filter(
      (value) => value.length > 0
    );

    if (approvals.length < 2) {
      setKillError("Two distinct director approvals are required.");
      return;
    }

    if (approvals[0].toLowerCase() === approvals[1].toLowerCase()) {
      setKillError("Director approvals must belong to different directors.");
      return;
    }

    if (!killArmed) {
      setKillArmed(true);
      setKillMessage(
        "Kill switch armed. Re-submit to confirm and broadcast the shutdown."
      );
      return;
    }

    if (killAccount.trim().length === 0) {
      setKillError("Please specify an account identifier.");
      return;
    }

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
        },
      });

      if (!response.ok) {
        throw new Error(`Kill switch request failed (${response.status})`);
      }

      const payload = (await response.json()) as KillSwitchResponse;
      setKillMessage(
        `Kill switch engaged${
          payload.ts ? ` at ${formatTimestamp(payload.ts)}` : ""
        }.`
      );
      setKillArmed(false);
      setFirstApproval("");
      setSecondApproval("");
      await refreshAuditTrail();
    } catch (error) {
      console.error("Kill switch request failed", error);
      setKillError(
        "Unable to engage the kill switch. Please verify connectivity and try again."
      );
    } finally {
      setKillLoading(false);
    }
  };

  const handleOverride = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    setOverrideError(null);
    setOverrideMessage(null);

    if (overrideAsset.trim().length === 0) {
      setOverrideError("Please provide an asset symbol.");
      return;
    }

    if (overrideReason.trim().length === 0) {
      setOverrideError("Please provide a reason for the override decision.");
      return;
    }

    const payload: AssetOverridePayload = {
      asset: overrideAsset.trim().toUpperCase(),
      reason: overrideReason.trim(),
      action: overrideAction,
    };

    setOverrideLoading(true);

    try {
      const response = await fetch("/director/assets/override", {
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
        overrideAction === "block"
          ? `${payload.asset} has been blocked.`
          : `${payload.asset} has been unblocked.`
      );
      setOverrideAsset("");
      setOverrideReason("");
      await refreshAuditTrail();
    } catch (error) {
      console.error("Override action failed", error);
      setOverrideError(
        "Unable to submit the override decision. Please try again later."
      );
    } finally {
      setOverrideLoading(false);
    }
  };

  return (
    <section className="director-controls">
      <h2>Director Controls</h2>

      <div className="control-card">
        <h3>Safe Mode</h3>
        <p>
          Current status: <strong>{safeModeStatusLabel}</strong>
          {safeModeStatus?.since && (
            <span>
              {" "}
              since {formatTimestamp(safeModeStatus.since)}
            </span>
          )}
          {safeModeStatus?.reason && (
            <span>
              {" "}- Reason: <em>{safeModeStatus.reason}</em>
            </span>
          )}
        </p>
        {safeModeLoading && <p>Loading safe mode status…</p>}
        {safeModeError && <p className="error">{safeModeError}</p>}
        <div className="form-grid">
          <label htmlFor="safe-mode-reason">Reason</label>
          <input
            id="safe-mode-reason"
            type="text"
            value={safeModeReason}
            onChange={(event) => setSafeModeReason(event.target.value)}
            placeholder="Reason for entering safe mode"
          />
          <label htmlFor="safe-mode-actor">Actor (optional)</label>
          <input
            id="safe-mode-actor"
            type="text"
            value={safeModeActor}
            onChange={(event) => setSafeModeActor(event.target.value)}
            placeholder="Recorded actor"
          />
        </div>
        <div className="button-row">
          <button
            type="button"
            onClick={() => handleSafeMode("enter")}
            disabled={safeModeActionLoading}
          >
            Enter Safe Mode
          </button>
          <button
            type="button"
            onClick={() => handleSafeMode("exit")}
            disabled={safeModeActionLoading}
          >
            Exit Safe Mode
          </button>
        </div>
        {safeModeMessage && <p className="success">{safeModeMessage}</p>}
        {safeModeActionError && <p className="error">{safeModeActionError}</p>}
      </div>

      <div className="control-card">
        <h3>Kill Switch</h3>
        <form onSubmit={handleKillSwitch}>
          <div className="form-grid">
            <label htmlFor="kill-account">Account</label>
            <input
              id="kill-account"
              type="text"
              value={killAccount}
              onChange={(event) => setKillAccount(event.target.value)}
              placeholder="Account identifier"
            />
            <label htmlFor="kill-reason">Reason</label>
            <select
              id="kill-reason"
              value={killReason}
              onChange={(event) =>
                setKillReason(event.target.value as KillSwitchReason)
              }
            >
              {KILL_SWITCH_REASONS.map((reason) => (
                <option key={reason.value} value={reason.value}>
                  {reason.label}
                </option>
              ))}
            </select>
            <label htmlFor="first-approval">Director Approval #1</label>
            <input
              id="first-approval"
              type="text"
              value={firstApproval}
              onChange={(event) => setFirstApproval(event.target.value)}
              placeholder="director-1"
            />
            <label htmlFor="second-approval">Director Approval #2</label>
            <input
              id="second-approval"
              type="text"
              value={secondApproval}
              onChange={(event) => setSecondApproval(event.target.value)}
              placeholder="director-2"
            />
          </div>
          <button type="submit" disabled={killLoading}>
            {killArmed ? "Confirm Kill Switch" : "Engage Kill Switch"}
          </button>
        </form>
        {killMessage && <p className="warning">{killMessage}</p>}
        {killError && <p className="error">{killError}</p>}
      </div>

      <div className="control-card">
        <h3>Override Decisions</h3>
        <form onSubmit={handleOverride}>
          <div className="form-grid">
            <label htmlFor="override-asset">Asset</label>
            <input
              id="override-asset"
              type="text"
              value={overrideAsset}
              onChange={(event) => setOverrideAsset(event.target.value)}
              placeholder="e.g. BTC-USD"
            />
            <label htmlFor="override-reason">Reason</label>
            <input
              id="override-reason"
              type="text"
              value={overrideReason}
              onChange={(event) => setOverrideReason(event.target.value)}
              placeholder="Why this override is needed"
            />
            <label htmlFor="override-action">Action</label>
            <select
              id="override-action"
              value={overrideAction}
              onChange={(event) =>
                setOverrideAction(event.target.value as "block" | "unblock")
              }
            >
              <option value="block">Block Asset</option>
              <option value="unblock">Unblock Asset</option>
            </select>
          </div>
          <button type="submit" disabled={overrideLoading}>
            Submit Override
          </button>
        </form>
        {overrideMessage && <p className="success">{overrideMessage}</p>}
        {overrideError && <p className="error">{overrideError}</p>}
      </div>

      <div className="control-card">
        <h3>Recent Director Actions</h3>
        {auditLoading && <p>Loading director actions…</p>}
        {auditError && <p className="error">{auditError}</p>}
        {!auditLoading && !auditError && auditTrail.length === 0 && (
          <p>No director actions recorded.</p>
        )}
        {!auditLoading && !auditError && auditTrail.length > 0 && (
          <table className="audit-table">
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Action</th>
                <th>Actor</th>
                <th>Target</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {auditTrail.map((entry, index) => (
                <tr key={entry.id ?? `${entry.action}-${index}`}>
                  <td>{formatTimestamp(entry.timestamp)}</td>
                  <td>{entry.action}</td>
                  <td>{entry.actor ?? "Unknown"}</td>
                  <td>{entry.target ?? "—"}</td>
                  <td>{entry.details ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
};

export default DirectorControls;
