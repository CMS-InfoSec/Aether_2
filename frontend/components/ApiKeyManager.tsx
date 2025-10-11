import React, { FormEvent, useCallback, useEffect, useState } from "react";
import { withErrorBoundary } from "./withErrorBoundary";
import { formatLondonTime } from "./timezone";
import { useAuthClaims } from "./useAuthClaims";

type SecretsStatusResponse = {
  last_rotated_at?: string | null;
  last_rotated_by?: string | null;
  status?: string | null;
};

type SecretsAuditEvent = {
  actor: string;
  rotated_at: string;
  notes?: string | null;
};

type RotateSecretsPayload = {
  api_key: string;
  api_secret: string;
};

const formatTimestamp = (timestamp?: string | null) => {
  if (!timestamp) {
    return "Never";
  }

  const formatted = formatLondonTime(timestamp);
  return formatted || "Never";
};

const ApiKeyManager: React.FC = () => {
  const [apiKey, setApiKey] = useState("");
  const [apiSecret, setApiSecret] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [statusLoading, setStatusLoading] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [lastRotatedAt, setLastRotatedAt] = useState<string | null>(null);
  const [lastRotatedBy, setLastRotatedBy] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [auditEvents, setAuditEvents] = useState<SecretsAuditEvent[]>([]);
  const [auditLoading, setAuditLoading] = useState<boolean>(true);
  const [auditError, setAuditError] = useState<string | null>(null);
  const [confirmationStage, setConfirmationStage] = useState<"idle" | "confirm">(
    "idle"
  );
  const [confirmationInput, setConfirmationInput] = useState("");
  const { readOnly, accessToken, mfaContext, mfaVerified } = useAuthClaims();
  const isAuthenticated = Boolean(accessToken);

  const loadStatus = useCallback(
    async (signal: AbortSignal) => {
      try {
        setStatusLoading(true);
        if (!isAuthenticated || !mfaVerified) {
          setStatusLoading(false);
          setStatusError(null);
          return;
        }

        const response = await fetch("/secrets/status", {
          method: "GET",
          headers: {
            Accept: "application/json",
            "X-MFA-Token": mfaContext,
            ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
          },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load status: ${response.status}`);
        }

        const data = (await response.json()) as SecretsStatusResponse;
        setLastRotatedAt(data.last_rotated_at ?? null);
        setLastRotatedBy(data.last_rotated_by ?? null);
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
    [accessToken, isAuthenticated, mfaContext, mfaVerified]
  );

  const refreshStatus = useCallback(async () => {
    const controller = new AbortController();
    try {
      await loadStatus(controller.signal);
    } finally {
      controller.abort();
    }
  }, [loadStatus]);

  const loadAudit = useCallback(
    async (signal: AbortSignal) => {
      try {
        setAuditLoading(true);
        if (!isAuthenticated || !mfaVerified) {
          setAuditLoading(false);
          setAuditError(null);
          return;
        }

        const response = await fetch("/secrets/audit", {
          method: "GET",
          headers: {
            Accept: "application/json",
            "X-MFA-Token": mfaContext,
            ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
          },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load audit history: ${response.status}`);
        }

        const data = (await response.json()) as SecretsAuditEvent[];
        setAuditEvents(data);
        setAuditError(null);
      } catch (error) {
        if (signal.aborted) {
          return;
        }

        console.error("Failed to fetch Kraken secret audit history", error);
        setAuditError("Unable to load the rotation history.");
      } finally {
        if (!signal.aborted) {
          setAuditLoading(false);
        }
      }
    },
    [accessToken, isAuthenticated, mfaContext, mfaVerified]
  );

  const refreshAudit = useCallback(async () => {
    const controller = new AbortController();
    try {
      await loadAudit(controller.signal);
    } finally {
      controller.abort();
    }
  }, [loadAudit]);

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

  useEffect(() => {
    const controller = new AbortController();

    loadAudit(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Failed to initialize secrets audit", error);
      }
    });

    return () => {
      controller.abort();
    };
  }, [loadAudit]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (readOnly) {
      setSubmitError("Auditor access is read-only. Credential rotation is disabled.");
      setSuccessMessage(null);
      return;
    }

    setSubmitError(null);
    setSuccessMessage(null);

    if (confirmationStage === "idle") {
      setConfirmationStage("confirm");
      return;
    }

    if (confirmationInput.trim().toUpperCase() !== "ROTATE") {
      setSubmitError('Please type "ROTATE" to confirm credential rotation.');
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
          "X-MFA-Token": mfaContext,
          ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
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
      setConfirmationStage("idle");
      setConfirmationInput("");
      await refreshStatus();
      await refreshAudit();
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

  if (!isAuthenticated) {
    return (
      <div className="max-w-xl mx-auto bg-white shadow rounded-lg p-6">
        <h2 className="text-2xl font-semibold text-gray-900 mb-4">Kraken API Key Manager</h2>
        <p className="text-sm text-gray-600">
          Please sign in with your corporate SSO account to manage Kraken API keys.
        </p>
      </div>
    );
  }

  if (!mfaVerified) {
    return (
      <div className="max-w-xl mx-auto bg-white shadow rounded-lg p-6">
        <h2 className="text-2xl font-semibold text-gray-900 mb-4">Kraken API Key Manager</h2>
        <p className="text-sm text-gray-600">
          Multi-factor authentication is required to rotate Kraken API credentials. Complete the MFA challenge to continue.
        </p>
      </div>
    );
  }

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
            {lastRotatedBy && (
              <p className="text-sm text-gray-500">Rotated by: {lastRotatedBy}</p>
            )}
            {statusMessage && (
              <p className="text-sm text-gray-500" role="status">
                Current status: {statusMessage}
              </p>
            )}
          </>
        )}
      </div>

      <form onSubmit={handleSubmit} className="space-y-4" aria-disabled={readOnly}>
        <fieldset disabled={readOnly} className="space-y-4">
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

          {confirmationStage === "confirm" && (
            <div className="rounded-md border border-yellow-300 bg-yellow-50 p-4">
              <p className="text-sm text-yellow-800 mb-2">
                Rotating the Kraken API credentials will immediately revoke access for the current key pair. Please confirm that
                you intend to proceed by typing <span className="font-semibold">ROTATE</span> below.
              </p>
              <input
                type="text"
                value={confirmationInput}
                onChange={(event) => setConfirmationInput(event.target.value)}
                className="w-full rounded-md border border-yellow-300 px-3 py-2 focus:border-yellow-500 focus:outline-none focus:ring-2 focus:ring-yellow-200"
                placeholder="Type ROTATE to confirm"
                autoComplete="off"
              />
              <div className="mt-3 flex items-center justify-between">
                <button
                  type="button"
                  className="text-sm font-medium text-gray-600 hover:text-gray-800"
                  onClick={() => {
                    setConfirmationStage("idle");
                    setConfirmationInput("");
                  }}
                >
                  Cancel rotation
                </button>
                <p className="text-xs text-gray-500">Confirmation required before submission</p>
              </div>
            </div>
          )}

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
              disabled={readOnly || isSubmitting}
              className="inline-flex items-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-75"
            >
              {isSubmitting ? "Rotating…" : confirmationStage === "confirm" ? "Confirm Rotation" : "Rotate Keys"}
            </button>
          </div>
        </fieldset>
      </form>

      {readOnly && (
        <div className="mt-4 rounded-md border border-gray-200 bg-gray-50 p-4 text-sm text-gray-600">
          Auditor access is read-only. Credential rotation is disabled.
        </div>
      )}

      <div className="mt-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Rotation History</h3>
        {auditLoading ? (
          <p className="text-sm text-gray-500">Loading rotation history…</p>
        ) : auditError ? (
          <p className="text-sm text-red-600" role="alert">
            {auditError}
          </p>
        ) : auditEvents.length === 0 ? (
          <p className="text-sm text-gray-500">No rotation events recorded.</p>
        ) : (
          <ul className="space-y-3">
            {auditEvents.map((event, index) => (
              <li key={`${event.rotated_at}-${index}`} className="rounded-md border border-gray-200 p-3">
                <p className="text-sm font-medium text-gray-900">
                  {formatTimestamp(event.rotated_at)}
                </p>
                <p className="text-sm text-gray-600">Rotated by {event.actor}</p>
                {event.notes && (
                  <p className="text-xs text-gray-500 mt-1">Notes: {event.notes}</p>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
};

export default withErrorBoundary(ApiKeyManager, {
  componentName: "API Key Manager",
});
