import React, { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { withErrorBoundary } from "./withErrorBoundary";
import { formatLondonTime } from "./timezone";
import { useAuthClaims } from "./useAuthClaims";

type KrakenStatusResponse = {
  rotated_at?: string | null;
};

type KrakenSecretPayload = {
  account_id: string;
  api_key: string;
  api_secret: string;
};

type AccountOption = {
  label: string;
  value: string;
};

const DEFAULT_ACCOUNT_OPTIONS: readonly AccountOption[] = [
  { label: "Company", value: "company" },
  { label: "Director 1", value: "director-1" },
  { label: "Director 2", value: "director-2" },
];

const formatTimestamp = (timestamp?: string | null) => {
  if (!timestamp) {
    return "Never";
  }

  const formatted = formatLondonTime(timestamp);
  return formatted || "Never";
};

const ApiKeyForm: React.FC = () => {
  const [apiKey, setApiKey] = useState("");
  const [apiSecret, setApiSecret] = useState("");
  const [accountId, setAccountId] = useState<string>("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [statusLoading, setStatusLoading] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [lastRotatedAt, setLastRotatedAt] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const { readOnly, accessToken, mfaContext, mfaVerified } = useAuthClaims();
  const isAuthenticated = Boolean(accessToken);
  const accountOptions = useMemo(() => {
    if (typeof window !== "undefined") {
      const adminAccounts = (window as typeof window & {
        __ADMIN_ACCOUNTS__?: string[];
      }).__ADMIN_ACCOUNTS__;

      if (Array.isArray(adminAccounts) && adminAccounts.length > 0) {
        return adminAccounts.map((value) => ({ label: value, value }));
      }
    }

    return [...DEFAULT_ACCOUNT_OPTIONS];
  }, []);

  const loadStatus = useCallback(
    async (signal: AbortSignal) => {
      if (!accountId) {
        setLastRotatedAt(null);
        setStatusError(null);
        setStatusLoading(false);
        return;
      }

      if (!isAuthenticated || !mfaVerified) {
        setStatusLoading(false);
        setStatusError(null);
        return;
      }

      try {
        setStatusLoading(true);
        const response = await fetch(
          `/secrets/kraken/status?account_id=${encodeURIComponent(accountId)}`,
          {
            method: "GET",
            headers: {
              Accept: "application/json",
              "X-Account-ID": accountId,
              "X-MFA-Context": mfaContext,
              ...(accessToken
                ? { Authorization: `Bearer ${accessToken}` }
                : {}),
            },
            signal,
          }
        );

        if (!response.ok) {
          if (response.status === 404) {
            setLastRotatedAt(null);
            setStatusError(null);
            return;
          }

          throw new Error(`Failed to load status: ${response.status}`);
        }

        const data = (await response.json()) as KrakenStatusResponse;
        setLastRotatedAt(data.rotated_at ?? null);
        setStatusError(null);
      } catch (error) {
        if (signal.aborted) {
          return;
        }

        console.error("Failed to fetch Kraken API key status", error);
        setStatusError("Unable to load the current rotation status.");
      } finally {
        if (!signal.aborted) {
          setStatusLoading(false);
        }
      }
    },
    [accountId, accessToken, isAuthenticated, mfaContext, mfaVerified]
  );

  const refreshStatus = useCallback(async () => {
    const controller = new AbortController();
    try {
      await loadStatus(controller.signal);
    } finally {
      controller.abort();
    }
  }, [loadStatus]);

  useEffect(() => {
    const controller = new AbortController();

    if (!accountId) {
      setStatusLoading(false);
      setLastRotatedAt(null);
      setStatusError(null);
      return () => {
        controller.abort();
      };
    }

    loadStatus(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Failed to initialize Kraken status", error);
      }
    });

    return () => {
      controller.abort();
    };
  }, [accountId, loadStatus]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (readOnly) {
      setSubmitError("Auditor access is read-only. Updating Kraken credentials is disabled.");
      setSuccessMessage(null);
      return;
    }
    if (!accountId) {
      setSubmitError("Please select an account before saving credentials.");
      return;
    }

    if (!isAuthenticated) {
      setSubmitError("SSO login required before rotating credentials.");
      return;
    }

    if (!mfaVerified) {
      setSubmitError("Complete multi-factor authentication to rotate credentials.");
      return;
    }

    setIsSubmitting(true);
    setSubmitError(null);
    setSuccessMessage(null);

    const payload: KrakenSecretPayload = {
      account_id: accountId,
      api_key: apiKey.trim(),
      api_secret: apiSecret.trim(),
    };

    try {
      const response = await fetch("/secrets/kraken", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "X-Account-ID": accountId,
          "X-MFA-Context": mfaContext,
          ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || "Failed to update Kraken API credentials.");
      }

      setSuccessMessage("Kraken API credentials updated successfully.");
      setApiSecret("");
      setApiKey("");
      await refreshStatus();
    } catch (error) {
      console.error("Failed to submit Kraken API credentials", error);
      setSubmitError(
        error instanceof Error ? error.message : "An unexpected error occurred."
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  if (!isAuthenticated) {
    return (
      <div className="max-w-xl mx-auto bg-white shadow rounded-lg p-6">
        <h2 className="text-2xl font-semibold text-gray-900 mb-4">Kraken API Credentials</h2>
        <p className="text-sm text-gray-600">
          Please sign in with your corporate single sign-on provider to manage Kraken API credentials.
        </p>
      </div>
    );
  }

  if (!mfaVerified) {
    return (
      <div className="max-w-xl mx-auto bg-white shadow rounded-lg p-6">
        <h2 className="text-2xl font-semibold text-gray-900 mb-4">Kraken API Credentials</h2>
        <p className="text-sm text-gray-600">
          Multi-factor authentication is required. Complete the MFA challenge to enable credential management.
        </p>
      </div>
    );
  }

  return (
    <div className="max-w-xl mx-auto bg-white shadow rounded-lg p-6">
      <h2 className="text-2xl font-semibold text-gray-900 mb-4">
        Kraken API Credentials
      </h2>

      <div className="mb-6" aria-live="polite">
        <h3 className="text-sm font-medium text-gray-700">Last Rotated</h3>
        {!accountId ? (
          <p className="text-sm text-gray-500">
            Select an account to view the rotation status.
          </p>
        ) : statusLoading ? (
          <p className="text-sm text-gray-500">Loading status…</p>
        ) : statusError ? (
          <p className="text-sm text-red-600" role="alert">
            {statusError}
          </p>
        ) : (
          <p className="text-sm text-gray-900">{formatTimestamp(lastRotatedAt)}</p>
        )}
      </div>

      <form onSubmit={handleSubmit} className="space-y-4" aria-disabled={readOnly}>
        <fieldset disabled={readOnly} className="space-y-4">
          <div>
            <label
              htmlFor="accountId"
              className="block text-sm font-medium text-gray-700 mb-1"
            >
              Account
            </label>
            <select
              id="accountId"
              name="accountId"
              value={accountId}
              onChange={(event) => {
                const newAccountId = event.target.value;
                setAccountId(newAccountId);
                setSubmitError(null);
                setSuccessMessage(null);
                setStatusError(null);
                if (!newAccountId) {
                  setStatusLoading(false);
                  setLastRotatedAt(null);
                } else {
                  setStatusLoading(true);
                }
              }}
              className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200"
              required
            >
              <option value="" disabled>
                Select an account
              </option>
              {accountOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label
              htmlFor="apiKey"
              className="block text-sm font-medium text-gray-700 mb-1"
            >
              API Key
            </label>
            <input
              id="apiKey"
              name="apiKey"
              type="text"
              value={apiKey}
              onChange={(event) => setApiKey(event.target.value)}
              className="w-full rounded-md border border-gray-300 px-3 py-2 focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200"
              placeholder="Enter your Kraken API key"
              required
              autoComplete="off"
            />
          </div>

          <div>
            <label
              htmlFor="apiSecret"
              className="block text-sm font-medium text-gray-700 mb-1"
            >
              API Secret
            </label>
            <input
              id="apiSecret"
              name="apiSecret"
              type="password"
              value={apiSecret}
              onChange={(event) => setApiSecret(event.target.value)}
              className="w-full rounded-md border border-gray-300 px-3 py-2 focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200"
              placeholder="Enter your Kraken API secret"
              required
              autoComplete="new-password"
            />
          </div>

          {submitError && (
            <div
              className="rounded-md bg-red-50 p-3 text-sm text-red-700"
              role="alert"
            >
              {submitError}
            </div>
          )}

          {successMessage && (
            <div
              className="rounded-md bg-green-50 p-3 text-sm text-green-700"
              role="status"
              aria-live="polite"
            >
              {successMessage}
            </div>
          )}

          <div className="flex justify-end">
            <button
              type="submit"
              disabled={readOnly || isSubmitting}
              className="inline-flex items-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-75"
            >
              {isSubmitting ? "Saving…" : "Save Credentials"}
            </button>
          </div>
        </fieldset>
      </form>

      {readOnly && (
        <div className="mt-4 rounded-md border border-gray-200 bg-gray-50 p-4 text-sm text-gray-600">
          Auditor access is read-only. Updating Kraken credentials is disabled.
        </div>
      )}
    </div>
  );
};

export default withErrorBoundary(ApiKeyForm, { componentName: "API Key Form" });
