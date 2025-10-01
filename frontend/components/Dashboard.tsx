import React, { useEffect, useMemo, useState } from "react";

interface Exposure {
  asset: string;
  value: number;
  direction?: "long" | "short";
}

interface TradeExplorerRow {
  intent: string;
  decision: string;
  fill: string;
  outcome: string;
}

interface RiskForecastResponse {
  nav?: number;
  realizedPnl?: number;
  unrealizedPnl?: number;
  var?: number;
  exposures?: Exposure[];
  trades?: TradeExplorerRow[];
}

interface FeesResponse {
  totalFees?: number;
  breakdown?: Record<string, number>;
}

interface BenchmarkResponse {
  benchmarkReturn?: number;
  relativePerformance?: number;
}

interface DashboardData {
  risk: RiskForecastResponse | null;
  fees: FeesResponse | null;
  benchmark: BenchmarkResponse | null;
}

const defaultData: DashboardData = {
  risk: null,
  fees: null,
  benchmark: null,
};

const numberFormatter = new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const percentFormatter = new Intl.NumberFormat(undefined, {
  style: "percent",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const Dashboard: React.FC = () => {
  const [data, setData] = useState<DashboardData>(defaultData);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    const controller = new AbortController();

    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const [riskRes, feesRes, benchmarkRes] = await Promise.all([
          fetch("/risk/forecast", { signal: controller.signal }),
          fetch("/fees/effective", { signal: controller.signal }),
          fetch("/benchmark/compare", { signal: controller.signal }),
        ]);

        if (!riskRes.ok || !feesRes.ok || !benchmarkRes.ok) {
          throw new Error("Unable to load dashboard data");
        }

        const [risk, fees, benchmark] = await Promise.all([
          riskRes.json(),
          feesRes.json(),
          benchmarkRes.json(),
        ]);

        if (isMounted) {
          setData({ risk, fees, benchmark });
        }
      } catch (err) {
        if (isMounted) {
          setError(err instanceof Error ? err.message : "Unknown error");
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

  const trades: TradeExplorerRow[] = useMemo(
    () => data.risk?.trades ?? [],
    [data.risk?.trades]
  );

  const csvContent = useMemo(() => {
    if (!trades.length) return "";

    const header = ["Intent", "Decision", "Fill", "Outcome"].join(",");
    const rows = trades.map((trade) =>
      [trade.intent, trade.decision, trade.fill, trade.outcome]
        .map((value) => `"${value?.replace(/"/g, '""')}"`)
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
      <nav className="sticky top-0 z-10 flex items-center justify-between border-b border-slate-800 bg-slate-950/80 px-6 py-4 backdrop-blur">
        <div className="text-lg font-semibold">Portfolio Dashboard</div>
        <div className="flex items-center gap-4 text-sm text-slate-300">
          <span>Updated just now</span>
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

        <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {metricCard("NAV", data.risk?.nav)}
          {metricCard("Realized PnL", data.risk?.realizedPnl)}
          {metricCard("Unrealized PnL", data.risk?.unrealizedPnl)}
          {metricCard("Value at Risk", data.risk?.var)}
        </section>

        <section className="grid gap-4 md:grid-cols-2">
          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
            <h2 className="text-lg font-semibold text-white">Exposures</h2>
            <div className="mt-4 overflow-hidden rounded-lg border border-slate-800">
              <table className="min-w-full divide-y divide-slate-800 text-sm">
                <thead className="bg-slate-900/80">
                  <tr>
                    <th className="px-4 py-2 text-left font-medium text-slate-300">Asset</th>
                    <th className="px-4 py-2 text-left font-medium text-slate-300">Exposure</th>
                    <th className="px-4 py-2 text-left font-medium text-slate-300">Direction</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {data.risk?.exposures?.length ? (
                    data.risk.exposures.map((exposure, index) => (
                      <tr key={`${exposure.asset}-${index}`} className="hover:bg-slate-900/40">
                        <td className="px-4 py-2 text-slate-200">{exposure.asset}</td>
                        <td className="px-4 py-2 text-slate-200">
                          {numberFormatter.format(exposure.value)}
                        </td>
                        <td className="px-4 py-2 text-slate-200">
                          {exposure.direction ? exposure.direction.toUpperCase() : "--"}
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td
                        colSpan={3}
                        className="px-4 py-6 text-center text-slate-400"
                      >
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
              <h2 className="text-lg font-semibold text-white">Fees</h2>
              <div className="mt-4 space-y-3 text-sm">
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Total Fees</span>
                  <span className="font-semibold text-white">
                    {data.fees?.totalFees !== undefined
                      ? numberFormatter.format(data.fees.totalFees)
                      : "--"}
                  </span>
                </div>
                {data.fees?.breakdown && (
                  <div className="space-y-2">
                    {Object.entries(data.fees.breakdown).map(([key, value]) => (
                      <div
                        key={key}
                        className="flex items-center justify-between rounded-lg bg-slate-900/80 px-3 py-2"
                      >
                        <span className="text-slate-400">{key}</span>
                        <span className="font-medium text-slate-100">
                          {numberFormatter.format(value)}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6">
              <h2 className="text-lg font-semibold text-white">Benchmark</h2>
              <div className="mt-4 space-y-3 text-sm">
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Benchmark Return</span>
                  <span className="font-semibold text-white">
                    {data.benchmark?.benchmarkReturn !== undefined
                      ? percentFormatter.format(data.benchmark.benchmarkReturn)
                      : "--"}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Relative Performance</span>
                  <span className="font-semibold text-white">
                    {data.benchmark?.relativePerformance !== undefined
                      ? percentFormatter.format(data.benchmark.relativePerformance)
                      : "--"}
                  </span>
                </div>
              </div>
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
                  <th className="px-4 py-3 text-left font-medium text-slate-300">Outcome</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {trades.length ? (
                  trades.map((trade, index) => (
                    <tr key={`${trade.intent}-${index}`} className="hover:bg-slate-900/40">
                      <td className="px-4 py-3 text-slate-200">{trade.intent}</td>
                      <td className="px-4 py-3 text-slate-200">{trade.decision}</td>
                      <td className="px-4 py-3 text-slate-200">{trade.fill}</td>
                      <td className="px-4 py-3 text-slate-200">{trade.outcome}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td
                      colSpan={4}
                      className="px-4 py-6 text-center text-slate-400"
                    >
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
