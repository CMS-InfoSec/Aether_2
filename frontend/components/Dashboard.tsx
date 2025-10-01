import React, { useEffect, useMemo, useState } from "react";

interface ExposureBreakdown {
  asset: string;
  netExposure: number;
  grossExposure?: number;
  direction?: "long" | "short";
}

interface TradeExplorerRow {
  intent: string;
  decision: string;
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

interface FeesEffectiveResponse {
  totalFees?: number;
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

interface DashboardState {
  risk: RiskForecastResponse | null;
  fees: FeesEffectiveResponse | null;
  universe: UniverseApprovedResponse | null;
  reportGeneratedAt: string | null;
}

const DEFAULT_ACCOUNT_ID = "ACC-DEFAULT";

const defaultState: DashboardState = {
  risk: null,
  fees: null,
  universe: null,
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
  const fillIndex = lowerHeader.findIndex((column) =>
    ["fill", "fill_price", "fill_qty", "fill_amount"].includes(column)
  );
  const pnlIndex = lowerHeader.findIndex((column) =>
    ["pnl", "p&l", "profit", "profit_loss"].includes(column)
  );

  if (intentIndex === -1 || decisionIndex === -1 || fillIndex === -1 || pnlIndex === -1) {
    return [];
  }

  return dataRows
    .filter((row) => row.length === header.length)
    .map((row) => ({
      intent: row[intentIndex] ?? "",
      decision: row[decisionIndex] ?? "",
      fill: row[fillIndex] ?? "",
      pnl: row[pnlIndex] ?? "",
    }))
    .filter((row) => row.intent || row.decision || row.fill || row.pnl);
};

const Dashboard: React.FC = () => {
  const [state, setState] = useState<DashboardState>(defaultState);
  const [trades, setTrades] = useState<TradeExplorerRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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

      const feesRequest = fetch("/fees/effective", { signal: controller.signal }).then(
        async (response) => {
          if (!response.ok) {
            throw new Error("Effective fees unavailable");
          }
          return response.json() as Promise<FeesEffectiveResponse>;
        }
      );

      const universeRequest = fetch("/universe/approved", { signal: controller.signal }).then(
        async (response) => {
          if (!response.ok) {
            throw new Error("Approved universe unavailable");
          }
          return response.json() as Promise<UniverseApprovedResponse>;
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
        const [riskResult, feesResult, universeResult, reportResult] = await Promise.allSettled([
          riskRequest,
          feesRequest,
          universeRequest,
          reportRequest,
        ]);

        if (!isMounted) {
          return;
        }

        const nextState: DashboardState = {
          risk: riskResult.status === "fulfilled" ? riskResult.value : null,
          fees: feesResult.status === "fulfilled" ? feesResult.value : null,
          universe: universeResult.status === "fulfilled" ? universeResult.value : null,
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

        const failures = [riskResult, feesResult, universeResult, reportResult]
          .map((result) => (result.status === "rejected" ? result.reason : null))
          .filter(Boolean) as Error[];

        if (failures.length) {
          setError(
            failures
              .map((failure) => (failure instanceof Error ? failure.message : String(failure)))
              .join("; ")
          );
        }
      } catch (err) {
        if (isMounted) {
          setError(err instanceof Error ? err.message : "Unable to load dashboard data");
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
    return state.risk.exposures.reduce((acc, exposure) => acc + Math.abs(exposure.netExposure), 0);
  }, [state.risk?.exposures]);

  const csvContent = useMemo(() => {
    if (!trades.length) {
      return "";
    }

    const header = ["Intent", "Decision", "Fill", "PnL"].join(",");
    const rows = trades.map((trade) =>
      [trade.intent, trade.decision, trade.fill, trade.pnl]
        .map((value) => `"${(value ?? "").replace(/"/g, '""')}"`)
        .join(",")
    );

    return [header, ...rows].join("\n");
  }, [trades]);

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

        <section className="grid gap-4 lg:grid-cols-3">
          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6 lg:col-span-2">
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
                        exposure.direction ?? (exposure.netExposure >= 0 ? "long" : "short");
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
              <h2 className="text-lg font-semibold text-white">Effective Fees</h2>
              <div className="mt-4 space-y-3 text-sm">
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Total Fees</span>
                  <span className="font-semibold text-white">
                    {state.fees?.totalFees !== undefined
                      ? currencyFormatter.format(state.fees.totalFees)
                      : "--"}
                  </span>
                </div>
                {state.fees?.components && (
                  <div className="space-y-2">
                    {Object.entries(state.fees.components).map(([key, value]) => (
                      <div
                        key={key}
                        className="flex items-center justify-between rounded-lg bg-slate-900/80 px-3 py-2"
                      >
                        <span className="text-slate-400">{toTitle(key)}</span>
                        <span className="font-medium text-slate-100">
                          {currencyFormatter.format(value)}
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

        <section className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
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
                  <th className="px-4 py-3 text-left font-medium text-slate-300">Fill</th>
                  <th className="px-4 py-3 text-left font-medium text-slate-300">PnL</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {trades.length ? (
                  trades.map((trade, index) => (
                    <tr key={`${trade.intent}-${index}`} className="hover:bg-slate-900/40">
                      <td className="px-4 py-3 text-slate-200">{trade.intent || "--"}</td>
                      <td className="px-4 py-3 text-slate-200">{trade.decision || "--"}</td>
                      <td className="px-4 py-3 text-slate-200">{trade.fill || "--"}</td>
                      <td className="px-4 py-3 text-slate-200">{trade.pnl || "--"}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={4} className="px-4 py-6 text-center text-slate-400">
                      No trade history available.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
};

export default Dashboard;
