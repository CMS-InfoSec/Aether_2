import React, { useEffect, useMemo, useState } from "react";
import { useAuthClaims } from "./useAuthClaims";

interface PortfolioData {
  positions: unknown;
  pnl: unknown;
  orders: unknown;
  fills: unknown;
}

const createDefaultPortfolioData = (): PortfolioData => ({
  positions: null,
  pnl: null,
  orders: null,
  fills: null,
});

class ForbiddenError extends Error {
  constructor(message = "Access denied") {
    super(message);
    this.name = "ForbiddenError";
  }
}

const parseAccountScopes = (input: unknown): string[] => {
  if (!input) {
    return [];
  }

  if (Array.isArray(input)) {
    return Array.from(
      new Set(
        input
          .map((value) => (typeof value === "string" ? value.trim() : ""))
          .filter((value): value is string => value.length > 0)
      )
    ).sort();
  }

  if (typeof input === "string") {
    const trimmed = input.trim();
    if (!trimmed) {
      return [];
    }

    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parseAccountScopes(parsed);
      }
    } catch (error) {
      // Swallow JSON parse error and fall through to comma splitting
    }

    return Array.from(
      new Set(
        trimmed
          .split(",")
          .map((value) => value.trim())
          .filter((value) => value.length > 0)
      )
    ).sort();
  }

  return [];
};

const PortfolioView: React.FC = () => {
  const { claims } = useAuthClaims();

  const accountScopes = useMemo(() => parseAccountScopes(claims?.account_scopes), [
    claims,
  ]);

  const [selectedAccount, setSelectedAccount] = useState<string | null>(() => {
    if (typeof window === "undefined") {
      return null;
    }
    const params = new URLSearchParams(window.location.search);
    const fromParams = params.get("account_id");
    return fromParams && fromParams.trim().length > 0 ? fromParams.trim() : null;
  });

  const [portfolioData, setPortfolioData] = useState<PortfolioData>(() =>
    createDefaultPortfolioData()
  );
  const [activeAccount, setActiveAccount] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const normalizedSelection = useMemo(
    () => (selectedAccount ? selectedAccount.trim() : ""),
    [selectedAccount]
  );

  useEffect(() => {
    if (selectedAccount || accountScopes.length === 0) {
      return;
    }
    setSelectedAccount(accountScopes[0] ?? null);
  }, [accountScopes, selectedAccount]);

  useEffect(() => {
    if (typeof window === "undefined" || !normalizedSelection) {
      return;
    }
    const url = new URL(window.location.href);
    url.searchParams.set("account_id", normalizedSelection);
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }, [normalizedSelection]);

  useEffect(() => {
    if (!normalizedSelection) {
      setPortfolioData(createDefaultPortfolioData());
      setActiveAccount(null);
      setError(null);
      return;
    }

    const controller = new AbortController();
    let cancelled = false;

    const fetchWithAccount = async (path: string) => {
      if (typeof window === "undefined") {
        throw new Error("Portfolio view is only available in browser contexts");
      }

      const url = new URL(path, window.location.origin);
      url.searchParams.set("account_id", normalizedSelection);

      const response = await fetch(url.toString(), { signal: controller.signal });

      if (response.status === 403) {
        throw new ForbiddenError();
      }

      if (response.status === 204) {
        return null;
      }

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(
          `Request to ${path} failed with status ${response.status}. ${text}`.trim()
        );
      }

      const contentType = response.headers.get("content-type");
      if (contentType && contentType.includes("application/json")) {
        return response.json();
      }

      return response.text();
    };

    const fetchPortfolio = async () => {
      setLoading(true);
      setError(null);
      try {
        const [positions, pnl, orders, fills] = await Promise.all([
          fetchWithAccount("/portfolio/positions"),
          fetchWithAccount("/portfolio/pnl"),
          fetchWithAccount("/portfolio/orders"),
          fetchWithAccount("/portfolio/fills"),
        ]);

        if (!cancelled) {
          setPortfolioData({ positions, pnl, orders, fills });
          setActiveAccount(normalizedSelection);
        }
      } catch (cause) {
        if (cancelled) {
          return;
        }

        if (cause instanceof ForbiddenError) {
          setError("Access denied");
        } else if (cause instanceof DOMException && cause.name === "AbortError") {
          // Swallow abort errors
        } else {
          console.error("Failed to fetch portfolio data", cause);
          setError("Failed to load portfolio data");
        }
        setPortfolioData(createDefaultPortfolioData());
        setActiveAccount(null);
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    fetchPortfolio().catch((cause) => {
      if (cause instanceof ForbiddenError) {
        setError("Access denied");
      } else if (cause instanceof DOMException && cause.name === "AbortError") {
        // Ignore abort
      } else {
        console.error("Failed to initialize portfolio fetch", cause);
        setError("Failed to load portfolio data");
      }
      setPortfolioData(createDefaultPortfolioData());
      setActiveAccount(null);
      setLoading(false);
    });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [normalizedSelection]);

  const isAccountPermitted = useMemo(
    () =>
      normalizedSelection && accountScopes.length
        ? accountScopes.includes(normalizedSelection)
        : false,
    [accountScopes, normalizedSelection]
  );

  if (!accountScopes.length) {
    return (
      <div className="portfolio-view">
        <h2>Portfolio</h2>
        <p>No account scopes available for this user.</p>
      </div>
    );
  }

  return (
    <div className="portfolio-view">
      <div className="portfolio-controls">
        <label htmlFor="portfolio-account-select">Account</label>
        <select
          id="portfolio-account-select"
          value={isAccountPermitted && normalizedSelection ? normalizedSelection : ""}
          onChange={(event) => {
            const value = event.target.value;
            setSelectedAccount(value.length > 0 ? value : null);
          }}
        >
          {!isAccountPermitted && normalizedSelection ? (
            <option value="" disabled>
              Unauthorized account selected
            </option>
          ) : null}
          {accountScopes.map((accountId) => (
            <option key={accountId} value={accountId}>
              {accountId}
            </option>
          ))}
        </select>
      </div>

      {normalizedSelection && !isAccountPermitted ? (
        <div className="portfolio-warning">
          <p>
            Access to account <strong>{normalizedSelection}</strong> is restricted. Please select an
            authorized account.
          </p>
        </div>
      ) : null}

      {loading ? <p>Loading portfolio data…</p> : null}
      {error ? <p className="portfolio-error">{error}</p> : null}

      {!loading && !error && activeAccount ? (
        <div className="portfolio-data">
          <section>
            <h3>Positions</h3>
            <pre>{JSON.stringify(portfolioData.positions, null, 2)}</pre>
          </section>
          <section>
            <h3>PnL</h3>
            <pre>{JSON.stringify(portfolioData.pnl, null, 2)}</pre>
          </section>
          <section>
            <h3>Orders</h3>
            <pre>{JSON.stringify(portfolioData.orders, null, 2)}</pre>
          </section>
          <section>
            <h3>Fills</h3>
            <pre>{JSON.stringify(portfolioData.fills, null, 2)}</pre>
          </section>
        </div>
      ) : null}
    </div>
  );
};

export default PortfolioView;
