import React, { FormEvent, useEffect, useState } from "react";

type KrakenStatusResponse = {
  lastRotatedAt?: string | null;
};

type KrakenSecretPayload = {
  apiKey: string;
  apiSecret: string;
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

const ApiKeyForm: React.FC = () => {
  const [apiKey, setApiKey] = useState("");
  const [apiSecret, setApiSecret] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [statusLoading, setStatusLoading] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [lastRotatedAt, setLastRotatedAt] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    const controller = new AbortController();

    const loadStatus = async () => {
      try {
        setStatusLoading(true);
        const response = await fetch("/secrets/kraken/status", {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
          signal: controller.signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load status: ${response.status}`);
        }

        const data = (await response.json()) as KrakenStatusResponse;

        if (isMounted) {
          setLastRotatedAt(data.lastRotatedAt ?? null);
          setStatusError(null);
        }
      } catch (error) {
        if (!isMounted || controller.signal.aborted) {
          return;
        }

        console.error("Failed to fetch Kraken API key status", error);
        setStatusError("Unable to load the current rotation status.");
      } finally {
        if (isMounted) {
          setStatusLoading(false);
        }
      }
    };

    loadStatus();

    return () => {
      isMounted = false;
      controller.abort();
    };
  }, []);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSubmitting(true);
    setSubmitError(null);
    setSuccessMessage(null);

    const payload: KrakenSecretPayload = {
      apiKey: apiKey.trim(),
      apiSecret: apiSecret.trim(),
    };

    try {
      const response = await fetch("/secrets/kraken", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
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
      setLastRotatedAt(new Date().toISOString());
    } catch (error) {
      console.error("Failed to submit Kraken API credentials", error);
      setSubmitError(
        error instanceof Error ? error.message : "An unexpected error occurred."
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="max-w-xl mx-auto bg-white shadow rounded-lg p-6">
      <h2 className="text-2xl font-semibold text-gray-900 mb-4">
        Kraken API Credentials
      </h2>

      <div className="mb-6">
        <h3 className="text-sm font-medium text-gray-700">Last Rotated</h3>
        {statusLoading ? (
          <p className="text-sm text-gray-500">Loading status…</p>
        ) : statusError ? (
          <p className="text-sm text-red-600">{statusError}</p>
        ) : (
          <p className="text-sm text-gray-900">{formatTimestamp(lastRotatedAt)}</p>
        )}
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
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
          <div className="rounded-md bg-red-50 p-3 text-sm text-red-700">
            {submitError}
          </div>
        )}

        {successMessage && (
          <div className="rounded-md bg-green-50 p-3 text-sm text-green-700">
            {successMessage}
          </div>
        )}

        <div className="flex justify-end">
          <button
            type="submit"
            disabled={isSubmitting}
            className="inline-flex items-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-75"
          >
            {isSubmitting ? "Saving…" : "Save Credentials"}
          </button>
        </div>
      </form>
    </div>
  );
};

export default ApiKeyForm;
