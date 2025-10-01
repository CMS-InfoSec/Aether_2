import React, { FormEvent, useCallback, useEffect, useState } from "react";

type SecretsStatusResponse = {
  last_rotated_at?: string | null;
  status?: string | null;
};

type RotateSecretsPayload = {
  api_key: string;
  api_secret: string;
};

const formatTimestamp = (timestamp?: string | null) => {
  if (!timestamp) {
    return "Never";
  }

  const date = new Date(timestamp);

  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }

  return date.toLocaleString();
};

const ApiKeyManager: React.FC = () => {
  const [apiKey, setApiKey] = useState("");
  const [apiSecret, setApiSecret] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [statusLoading, setStatusLoading] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [lastRotatedAt, setLastRotatedAt] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const loadStatus = useCallback(
    async (signal: AbortSignal) => {
      try {
        setStatusLoading(true);
        const response = await fetch("/secrets/status", {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load status: ${response.status}`);
        }

        const data = (await response.json()) as SecretsStatusResponse;
        setLastRotatedAt(data.last_rotated_at ?? null);
        setStatusMessage(data.status ?? null);
        setStatusError(null);
      } catch (error) {
        if (signal.aborted) {
          return;
        }

        console.error("Failed to fetch Kraken secret status", error);
        setStatusError("Unable to load the current rotation status.");
      } finally {
        if (!signal.aborted) {
          setStatusLoading(false);
        }
      }
    },
    []
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

    loadStatus(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Failed to initialize secrets status", error);
      }
    });

    return () => {
      controller.abort();
    };
  }, [loadStatus]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    setIsSubmitting(true);
    setSubmitError(null);
    setSuccessMessage(null);

    const payload: RotateSecretsPayload = {
      api_key: apiKey.trim(),
      api_secret: apiSecret.trim(),
    };

    try {
      const response = await fetch("/secrets/rotate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        if (response.status === 400) {
          setSubmitError("Invalid API key or secret. Please verify your credentials and try again.");
          return;
        }

        const errorText = await response.text();
        throw new Error(errorText || "Failed to rotate Kraken API credentials.");
      }

      setSuccessMessage("Kraken API credentials rotated successfully.");
      setApiKey("");
      setApiSecret("");
      await refreshStatus();
    } catch (error) {
      console.error("Failed to rotate Kraken API credentials", error);
      setSubmitError(
        error instanceof Error
          ? error.message
          : "An unexpected error occurred while rotating credentials."
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="max-w-xl mx-auto bg-white shadow rounded-lg p-6">
      <h2 className="text-2xl font-semibold text-gray-900 mb-4">Kraken API Key Manager</h2>

      <div className="mb-6" aria-live="polite">
        <h3 className="text-sm font-medium text-gray-700">Last Rotated</h3>
        {statusLoading ? (
          <p className="text-sm text-gray-500">Loading status…</p>
        ) : statusError ? (
          <p className="text-sm text-red-600" role="alert">
            {statusError}
          </p>
        ) : (
          <>
            <p className="text-sm text-gray-900">{formatTimestamp(lastRotatedAt)}</p>
            {statusMessage && (
              <p className="text-sm text-gray-500" role="status">
                Current status: {statusMessage}
              </p>
            )}
          </>
        )}
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="apiKey" className="block text-sm font-medium text-gray-700 mb-1">
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
          <label htmlFor="apiSecret" className="block text-sm font-medium text-gray-700 mb-1">
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
          <div className="rounded-md bg-red-50 p-3 text-sm text-red-700" role="alert">
            {submitError}
          </div>
        )}

        {successMessage && (
          <div className="rounded-md bg-green-50 p-3 text-sm text-green-700" role="status" aria-live="polite">
            {successMessage}
          </div>
        )}

        <div className="flex justify-end">
          <button
            type="submit"
            disabled={isSubmitting}
            className="inline-flex items-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-75"
          >
            {isSubmitting ? "Rotating…" : "Rotate Keys"}
          </button>
        </div>
      </form>
    </div>
  );
};

export default ApiKeyManager;
