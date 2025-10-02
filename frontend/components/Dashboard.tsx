import React, { FormEvent, useEffect, useMemo, useState } from "react";
import { useAuthClaims } from "./useAuthClaims";

interface ExposureBreakdown {
  asset: string;
  netExposure: number;
  grossExposure?: number;
  direction?: "long" | "short";
}

interface TradeExplorerRow {
  intent: string;
  decision: string;
  order: string;
  fill: string;
  pnl: string;
}

interface RiskForecastResponse {
  nav?: number;
  realizedPnl?: number;
  unrealizedPnl?: number;
  valueAtRisk?: number;
  exposures?: ExposureBreakdown[];
}

interface FeesAccountSummaryResponse {
  account_id?: string;
  total_fees?: number;
  realized_fees?: number;
  unrealized_fees?: number;
  components?: Record<string, number>;
}

interface UniverseThresholds {
  cap: number;
  volume_global: number;
  volume_kraken: number;
  ann_vol: number;
}

interface UniverseApprovedResponse {
  symbols: string[];
  generated_at: string;
  thresholds: UniverseThresholds;
}

interface SafeModeStatusResponse {
  active: boolean;
  reason?: string | null;
  since?: string | null;
  actor?: string | null;
}

interface KrakenRotationResponse {
  rotated_at?: string;
  secret_name?: string;
}

interface DashboardState {
  risk: RiskForecastResponse | null;
  fees: FeesAccountSummaryResponse | null;
  universe: UniverseApprovedResponse | null;
  safeMode: SafeModeStatusResponse | null;
  reportGeneratedAt: string | null;
}

const DEFAULT_ACCOUNT_ID = "ACC-DEFAULT";

const defaultState: DashboardState = {
  risk: null,
  fees: null,
  universe: null,
  safeMode: null,
  reportGeneratedAt: null,
};

const numberFormatter = new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const currencyFormatter = new Intl.NumberFormat(undefined, {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

const percentFormatter = new Intl.NumberFormat(undefined, {
  style: "percent",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const toTitle = (value: string) =>
  value
    .split("_")
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(" ");

const parseCsv = (input: string): string[][] => {
  const rows: string[][] = [];
  let currentField = "";
  let currentRow: string[] = [];
  let insideQuotes = false;

  const pushField = () => {
    currentRow.push(currentField);
    currentField = "";
  };

  const pushRow = () => {
    rows.push(currentRow);
    currentRow = [];
  };

  for (let i = 0; i < input.length; i += 1) {
    const char = input[i];
    const nextChar = input[i + 1];

    if (char === "\"") {
      if (insideQuotes && nextChar === "\"") {
        currentField += "\"";
        i += 1;
      } else {
        insideQuotes = !insideQuotes;
      }
    } else if (char === "," && !insideQuotes) {
      pushField();
    } else if ((char === "\n" || char === "\r") && !insideQuotes) {
      if (char === "\r" && nextChar === "\n") {
        i += 1;
      }
      pushField();
      pushRow();
    } else {
      currentField += char;
    }
  }

  if (currentField.length > 0 || currentRow.length > 0) {
    pushField();
    pushRow();
  }

  return rows.filter((row) => row.length > 0);
};

const extractTradeRows = (csv: string): TradeExplorerRow[] => {
  const rows = parseCsv(csv);
  if (!rows.length) {
    return [];
  }

  const [header, ...dataRows] = rows;
  const lowerHeader = header.map((column) => column.trim().toLowerCase());

  const intentIndex = lowerHeader.findIndex((column) =>
    ["intent", "trade_intent", "signal_intent"].includes(column)
  );
  const decisionIndex = lowerHeader.findIndex((column) =>
    ["decision", "trade_decision"].includes(column)
  );
  const orderIndex = lowerHeader.findIndex((column) =>
    ["order", "order_id", "order_action"].includes(column)
  );
  const fillIndex = lowerHeader.findIndex((column) =>
    ["fill", "fill_price", "fill_qty", "fill_amount"].includes(column)
  );
  const pnlIndex = lowerHeader.findIndex((column) =>
    ["pnl", "p&l", "profit", "profit_loss"].includes(column)
  );

  if (
    intentIndex === -1 ||
    decisionIndex === -1 ||
    fillIndex === -1 ||
    pnlIndex === -1
  ) {
    return [];
  }

  return dataRows
    .filter((row) => row.length === header.length)
    .map((row) => ({
      intent: row[intentIndex] ?? "",
      decision: row[decisionIndex] ?? "",
      order: orderIndex !== -1 ? row[orderIndex] ?? "" : "",
      fill: row[fillIndex] ?? "",
      pnl: row[pnlIndex] ?? "",
    }))
    .filter((row) => row.intent || row.decision || row.order || row.fill || row.pnl);
};

const Dashboard: React.FC = () => {
  const [state, setState] = useState<DashboardState>(defaultState);
  const [trades, setTrades] = useState<TradeExplorerRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [apiKey, setApiKey] = useState("");
  const [apiSecret, setApiSecret] = useState("");
  const [rotationMessage, setRotationMessage] = useState<string | null>(null);
  const [rotationError, setRotationError] = useState<string | null>(null);
  const [rotationSubmitting, setRotationSubmitting] = useState(false);
  const { readOnly } = useAuthClaims();

  useEffect(() => {
    let isMounted = true;
    const controller = new AbortController();

    const fetchData = async () => {
      setLoading(true);
      setError(null);

      const riskRequest = fetch("/risk/forecast", { signal: controller.signal }).then(
        async (response) => {
          if (!response.ok) {
            throw new Error("Risk forecast unavailable");
          }
          return response.json() as Promise<RiskForecastResponse>;
        }
      );

      const feesRequest = fetch(
        `/fees/account_summary?account_id=${encodeURIComponent(DEFAULT_ACCOUNT_ID)}`,
        { signal: controller.signal }
      ).then(async (response) => {
        if (!response.ok) {
          throw new Error("Account fee summary unavailable");
        }
        return response.json() as Promise<FeesAccountSummaryResponse>;
      });

      const universeRequest = fetch("/universe/approved", { signal: controller.signal }).then(
        async (response) => {
          if (!response.ok) {
            throw new Error("Approved universe unavailable");
          }
          return response.json() as Promise<UniverseApprovedResponse>;
        }
      );

      const safeModeRequest = fetch("/safe_mode/status", { signal: controller.signal }).then(
        async (response) => {
          if (!response.ok) {
            throw new Error("Safe mode status unavailable");
          }
          return response.json() as Promise<SafeModeStatusResponse>;
        }
      );

      const reportRequest = fetch(
        `/reports/daily?account_id=${encodeURIComponent(DEFAULT_ACCOUNT_ID)}`,
        { signal: controller.signal }
      ).then(async (response) => {
        if (!response.ok) {
          throw new Error("Daily report unavailable");
        }
        return response.text();
      });

      try {
        const [riskResult, feesResult, universeResult, safeModeResult, reportResult] =
          await Promise.allSettled([
            riskRequest,
            feesRequest,
            universeRequest,
            safeModeRequest,
            reportRequest,
          ]);

        if (!isMounted) {
          return;
        }

        const nextState: DashboardState = {
          risk: riskResult.status === "fulfilled" ? riskResult.value : null,
          fees: feesResult.status === "fulfilled" ? feesResult.value : null,
          universe: universeResult.status === "fulfilled" ? universeResult.value : null,
          safeMode: safeModeResult.status === "fulfilled" ? safeModeResult.value : null,
          reportGeneratedAt:
            reportResult.status === "fulfilled" ? new Date().toISOString() : null,
        };
        setState(nextState);

        if (reportResult.status === "fulfilled") {
          const parsedTrades = extractTradeRows(reportResult.value);
          setTrades(parsedTrades);
        } else {
          setTrades([]);
        }

        const failures = [
          riskResult,
          feesResult,
          universeResult,
          safeModeResult,
          reportResult,
        ]
          .map((result) => (result.status === "rejected" ? result.reason : null))
          .filter(Boolean) as Error[];

        if (failures.length) {
          setError(
            failures
              .map((failure) =>
                failure instanceof Error ? failure.message : String(failure)
              )
              .join("; ")
          );
        }
      } catch (err) {
        if (isMounted) {
          setError(
            err instanceof Error ? err.message : "Unable to load dashboard data"
          );
          setState(defaultState);
          setTrades([]);
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    fetchData();

    return () => {
      isMounted = false;
      controller.abort();
    };
  }, []);

  const totalExposure = useMemo(() => {
    if (!state.risk?.exposures?.length) {
      return undefined;
    }
    return state.risk.exposures.reduce(
      (acc, exposure) => acc + Math.abs(exposure.netExposure),
      0
    );
  }, [state.risk?.exposures]);

  const csvContent = useMemo(() => {
    if (!trades.length) {
      return "";
    }

    const header = ["Intent", "Decision", "Order", "Fill", "PnL"].join(",");
    const rows = trades.map((trade) =>
      [trade.intent, trade.decision, trade.order, trade.fill, trade.pnl]
        .map((value) => `"${(value ?? "").replace(/"/g, '""')}"`)
        .join(",")
    );

    return [header, ...rows].join("\n");
  }, [trades]);

  const feeComponents = useMemo(() => {
    if (!state.fees) {
      return [] as { label: string; value: number }[];
    }

    const items: { label: string; value: number }[] = [];
    if (state.fees.realized_fees !== undefined) {
      items.push({ label: "Realized Fees", value: state.fees.realized_fees });
    }
    if (state.fees.unrealized_fees !== undefined) {
      items.push({ label: "Accrued Fees", value: state.fees.unrealized_fees });
    }

    if (state.fees.components) {
      Object.entries(state.fees.components).forEach(([key, value]) => {
        items.push({ label: toTitle(key), value });
      });
    }

    return items;
  }, [state.fees]);

  const handleRotateSecrets = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (readOnly) {
      setRotationError("Auditor access is read-only. Credential rotation is disabled.");
      setRotationMessage(null);
      return;
    }
    if (!apiKey || !apiSecret) {
      setRotationError("API key and secret are required");
      setRotationMessage(null);
      return;
    }

    setRotationSubmitting(true);
    setRotationError(null);
    setRotationMessage(null);

    try {
      const response = await fetch("/secrets/rotate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          account_id: DEFAULT_ACCOUNT_ID,
          api_key: apiKey,
          api_secret: apiSecret,
        }),
      });

      if (!response.ok) {
        throw new Error("Rotation failed");
      }

      const payload = (await response.json()) as KrakenRotationResponse;
      const rotatedAt = payload.rotated_at
        ? new Date(payload.rotated_at).toLocaleString()
        : null;

      setRotationMessage(
        rotatedAt
          ? `Kraken credentials rotated successfully at ${rotatedAt}.`
          : "Kraken credentials rotated successfully."
      );
      setApiKey("");
      setApiSecret("");
    } catch (err) {
      setRotationError(
        err instanceof Error ? err.message : "Unable to rotate Kraken credentials"
      );
    } finally {
      setRotationSubmitting(false);
    }
  };

  const safeModeStatusLabel = useMemo(() => {
    if (!state.safeMode) {
      return "Safe mode status unknown";
    }
    if (state.safeMode.active) {
      return `Safe mode active${
        state.safeMode.reason ? `: ${state.safeMode.reason}` : ""
      }`;
    }
    return "Safe mode inactive";
  }, [state.safeMode]);

  const handleExportCsv = () => {
    if (!csvContent) return;

    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", "trade-explorer.csv");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const metricCard = (
    label: string,
    value: number | undefined,
    formatter: Intl.NumberFormat = numberFormatter,
    fallback: string = "--"
  ) => (
    <div className="rounded-lg bg-slate-900/70 p-4 shadow-lg ring-1 ring-slate-800">
      <dt className="text-sm font-medium text-slate-300">{label}</dt>
      <dd className="mt-2 text-2xl font-semibold text-white">
        {value !== undefined ? formatter.format(value) : fallback}
      </dd>
    </div>
  );

  return (
    <div className="flex min-h-screen flex-col bg-slate-950 text-white">
      <nav className="sticky top-0 z-10 flex flex-wrap items-center justify-between gap-4 border-b border-slate-800 bg-slate-950/80 px-6 py-4 backdrop-blur">
        <div className="text-lg font-semibold">Risk Intelligence Dashboard</div>
        <div className="flex flex-wrap items-center gap-3 text-sm text-slate-300">
          <span
            className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium ${
              state.safeMode?.active
                ? "border-amber-400/60 bg-amber-400/10 text-amber-200"
                : "border-emerald-500/40 bg-emerald-500/10 text-emerald-200"
            }`}
          >
            <span
              className={`h-2 w-2 rounded-full ${
                state.safeMode?.active ? "bg-amber-400" : "bg-emerald-400"
              }`}
            />
            {safeModeStatusLabel}
          </span>
          {state.reportGeneratedAt && (
            <span>
              Report synced {new Date(state.reportGeneratedAt).toLocaleString()}
            </span>
          )}
          <button
            onClick={handleExportCsv}
            disabled={!csvContent}
            className="rounded-md bg-emerald-500 px-3 py-1.5 font-medium text-slate-950 transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
          >
            Export Trades CSV
          </button>
        </div>
      </nav>

      <main className="flex-1 space-y-8 px-6 py-8">
        {loading && (
          <div className="flex w-full justify-center">
            <span className="animate-pulse text-slate-400">Loading dashboard...</span>
          </div>
        )}

        {error && (
          <div className="rounded-md border border-rose-500/40 bg-rose-950/40 p-4 text-sm text-rose-200">
            {error}
          </div>
        )}

        <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          {metricCard("Net Asset Value", state.risk?.nav, currencyFormatter)}
          {metricCard("Realized PnL", state.risk?.realizedPnl, currencyFormatter)}
          {metricCard("Unrealized PnL", state.risk?.unrealizedPnl, currencyFormatter)}
          {metricCard("Value at Risk", state.risk?.valueAtRisk, currencyFormatter)}
          {metricCard("Total Exposure", totalExposure, currencyFormatter)}
        </section>

        <section className="grid gap-4 xl:grid-cols-3">
          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6 xl:col-span-2">
            <h2 className="text-lg font-semibold text-white">Exposure Breakdown</h2>
            <div className="mt-4 overflow-hidden rounded-lg border border-slate-800">
              <table className="min-w-full divide-y divide-slate-800 text-sm">
                <thead className="bg-slate-900/80">
                  <tr>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Asset</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Net Exposure</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Gross Exposure</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Direction</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {state.risk?.exposures?.length ? (
                    state.risk.exposures.map((exposure) => {
                      const derivedDirection =
                        exposure.direction ??
                        (exposure.netExposure >= 0 ? "long" : "short");
                      return (
                        <tr key={exposure.asset} className="hover:bg-slate-900/40">
                          <td className="px-4 py-3 text-slate-200">{exposure.asset}</td>
                          <td className="px-4 py-3 text-slate-200">
                            {numberFormatter.format(exposure.netExposure)}
                          </td>
                          <td className="px-4 py-3 text-slate-200">
                            {exposure.grossExposure !== undefined
                              ? numberFormatter.format(exposure.grossExposure)
                              : "--"}
                          </td>
                          <td className="px-4 py-3 text-slate-200 uppercase">
                            {derivedDirection}
                          </td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td colSpan={4} className="px-4 py-6 text-center text-slate-400">
                        No exposure data available.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="space-y-4">
            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
              <h2 className="text-lg font-semibold text-white">Fees Overview</h2>
              <div className="mt-4 space-y-3 text-sm">
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Total Fees</span>
                  <span className="font-semibold text-white">
                    {state.fees?.total_fees !== undefined
                      ? currencyFormatter.format(state.fees.total_fees)
                      : "--"}
                  </span>
                </div>
                {feeComponents.length > 0 && (
                  <div className="space-y-2">
                    {feeComponents.map((component) => (
                      <div
                        key={component.label}
                        className="flex items-center justify-between rounded-lg bg-slate-900/80 px-3 py-2"
                      >
                        <span className="text-slate-400">{component.label}</span>
                        <span className="font-medium text-slate-100">
                          {currencyFormatter.format(component.value)}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
              <h2 className="text-lg font-semibold text-white">Approved Universe</h2>
              {state.universe ? (
                <div className="mt-4 space-y-4 text-sm text-slate-300">
                  <div>
                    <span className="text-slate-400">Generated</span>
                    <div className="font-medium text-white">
                      {new Date(state.universe.generated_at).toLocaleString()}
                    </div>
                  </div>
                  <div className="space-y-2">
                    <div className="text-xs uppercase tracking-wide text-slate-500">
                      Thresholds
                    </div>
                    <ul className="space-y-1">
                      <li className="flex items-center justify-between">
                        <span>Market Cap</span>
                        <span>{currencyFormatter.format(state.universe.thresholds.cap)}</span>
                      </li>
                      <li className="flex items-center justify-between">
                        <span>Global Volume</span>
                        <span>
                          {currencyFormatter.format(state.universe.thresholds.volume_global)}
                        </span>
                      </li>
                      <li className="flex items-center justify-between">
                        <span>Kraken Volume</span>
                        <span>
                          {currencyFormatter.format(state.universe.thresholds.volume_kraken)}
                        </span>
                      </li>
                      <li className="flex items-center justify-between">
                        <span>Annualised Volatility</span>
                        <span>
                          {percentFormatter.format(state.universe.thresholds.ann_vol)}
                        </span>
                      </li>
                    </ul>
                  </div>
                  <div className="space-y-2">
                    <div className="text-xs uppercase tracking-wide text-slate-500">
                      Approved Symbols ({state.universe.symbols.length})
                    </div>
                    <div className="flex max-h-40 flex-wrap gap-2 overflow-y-auto rounded-lg border border-slate-800 bg-slate-900/40 p-3">
                      {state.universe.symbols.map((symbol) => (
                        <span
                          key={symbol}
                          className="rounded-full bg-slate-800 px-3 py-1 text-xs font-medium text-slate-100"
                        >
                          {symbol}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="mt-4 text-sm text-slate-400">
                  Universe data unavailable.
                </div>
              )}
            </div>
          </div>
        </section>

        <section className="grid gap-4 lg:grid-cols-[2fr_1fr]">
          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
            <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
              <h2 className="text-lg font-semibold text-white">Trade Explorer</h2>
              <div className="text-sm text-slate-400">
                {trades.length} trade{trades.length === 1 ? "" : "s"}
              </div>
            </div>

            <div className="mt-4 overflow-x-auto">
              <table className="min-w-full divide-y divide-slate-800 text-sm">
                <thead className="bg-slate-900/80">
                  <tr>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Intent</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Decision</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Order</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">Fill</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-300">PnL</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {trades.length ? (
                    trades.map((trade, index) => (
                      <tr
                        key={`${trade.intent}-${trade.decision}-${index}`}
                        className="hover:bg-slate-900/40"
                      >
                        <td className="px-4 py-3 text-slate-200">{trade.intent || "--"}</td>
                        <td className="px-4 py-3 text-slate-200">{trade.decision || "--"}</td>
                        <td className="px-4 py-3 text-slate-200">{trade.order || "--"}</td>
                        <td className="px-4 py-3 text-slate-200">{trade.fill || "--"}</td>
                        <td className="px-4 py-3 text-slate-200">{trade.pnl || "--"}</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={5} className="px-4 py-6 text-center text-slate-400">
                        No trade history available.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
            <h2 className="text-lg font-semibold text-white">Safe Mode &amp; Credentials</h2>
            <div className="mt-4 space-y-6 text-sm text-slate-300">
              <div className="space-y-2 rounded-lg border border-slate-800 bg-slate-900/70 p-4">
                <div className="flex items-center justify-between">
                  <span className="font-medium text-white">Safe Mode</span>
                  <span
                    className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs ${
                      state.safeMode?.active
                        ? "bg-amber-400/10 text-amber-200"
                        : "bg-emerald-400/10 text-emerald-200"
                    }`}
                  >
                    <span
                      className={`h-2 w-2 rounded-full ${
                        state.safeMode?.active ? "bg-amber-400" : "bg-emerald-400"
                      }`}
                    />
                    {state.safeMode?.active ? "Active" : "Inactive"}
                  </span>
                </div>
                <dl className="grid grid-cols-1 gap-3 text-xs text-slate-400">
                  <div>
                    <dt className="uppercase tracking-wide">Reason</dt>
                    <dd className="text-slate-200">{state.safeMode?.reason || "--"}</dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-wide">Engaged By</dt>
                    <dd className="text-slate-200">{state.safeMode?.actor || "--"}</dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-wide">Since</dt>
                    <dd className="text-slate-200">
                      {state.safeMode?.since
                        ? new Date(state.safeMode.since).toLocaleString()
                        : "--"}
                    </dd>
                  </div>
                </dl>
              </div>

              <form onSubmit={handleRotateSecrets} className="space-y-3" aria-disabled={readOnly}>
                <fieldset disabled={readOnly} className="space-y-3">
                  <div className="text-xs uppercase tracking-wide text-slate-500">
                    Rotate Kraken API Keys
                  </div>
                  <label className="block text-xs text-slate-400">
                    API Key
                    <input
                      type="text"
                      value={apiKey}
                      onChange={(event) => setApiKey(event.target.value)}
                      className="mt-1 w-full rounded-md border border-slate-800 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-500"
                      placeholder="Enter new API key"
                      autoComplete="off"
                    />
                  </label>
                  <label className="block text-xs text-slate-400">
                    API Secret
                    <input
                      type="password"
                      value={apiSecret}
                      onChange={(event) => setApiSecret(event.target.value)}
                      className="mt-1 w-full rounded-md border border-slate-800 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-500"
                      placeholder="Enter new API secret"
                      autoComplete="off"
                    />
                  </label>
                  <button
                    type="submit"
                    disabled={readOnly || rotationSubmitting}
                    className="w-full rounded-md bg-emerald-500 px-3 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:bg-slate-800 disabled:text-slate-400"
                  >
                    {rotationSubmitting ? "Rotating..." : "Rotate Credentials"}
                  </button>
                  {rotationError && (
                    <div className="rounded-md border border-rose-500/40 bg-rose-950/40 px-3 py-2 text-xs text-rose-200">
                      {rotationError}
                    </div>
                  )}
                  {rotationMessage && (
                    <div className="rounded-md border border-emerald-500/40 bg-emerald-950/30 px-3 py-2 text-xs text-emerald-200">
                      {rotationMessage}
                    </div>
                  )}
                </fieldset>
              </form>
              {readOnly && (
                <div className="rounded-md border border-slate-800 bg-slate-950/60 px-3 py-2 text-xs text-slate-300">
                  Auditor access is read-only. Credential rotation is disabled.
                </div>
              )}
            </div>
          </div>
        </section>

        <section className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
          <h2 className="text-lg font-semibold text-white">Trade Pipeline</h2>
          <div className="mt-6 space-y-6">
            {trades.length ? (
              trades.map((trade, index) => (
                <div
                  key={`${trade.intent}-${trade.order}-${trade.fill}-${index}`}
                  className="space-y-4 rounded-lg border border-slate-800 bg-slate-900/50 p-4"
                >
                  <div className="flex flex-col gap-2 text-sm md:flex-row md:items-center md:justify-between">
                    <div className="font-semibold text-white">{trade.intent || "Unnamed Intent"}</div>
                    <div className="text-xs uppercase tracking-wide text-slate-500">
                      PnL: <span className="font-medium text-slate-200">{trade.pnl || "--"}</span>
                    </div>
                  </div>
                  <div className="grid gap-4 md:grid-cols-4">
                    {[
                      { title: "Intent", value: trade.intent },
                      { title: "Decision", value: trade.decision },
                      { title: "Order", value: trade.order },
                      { title: "Fill", value: trade.fill },
                    ].map((step) => (
                      <div
                        key={step.title}
                        className="space-y-2 rounded-md border border-slate-800 bg-slate-900/60 p-3"
                      >
                        <div className="text-xs uppercase tracking-wide text-slate-400">
                          {step.title}
                        </div>
                        <div className="text-sm text-slate-100">
                          {step.value || "--"}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ))
            ) : (
              <div className="rounded-md border border-slate-800 bg-slate-900/50 p-6 text-center text-sm text-slate-400">
                No trade pipeline data available.
              </div>
            )}
          </div>
        </section>
      </main>
    </div>
  );
};

export default Dashboard;
