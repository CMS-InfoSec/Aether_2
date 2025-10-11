import React, { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { withErrorBoundary } from "./withErrorBoundary";
import { formatLondonTime } from "./timezone";
import { useAuthClaims } from "./useAuthClaims";

type AccountSummary = {
  account_id: string;
  name: string;
  owner_user_id: string;
  sim_mode: boolean;
  hedge_auto: boolean;
  active: boolean;
};

type AccountCreatePayload = {
  name: string;
  owner_user_id: string;
  base_currency: string;
  sim_mode: boolean;
  hedge_auto: boolean;
  initial_api_key?: string;
  initial_api_secret?: string;
};

type AccountCreateResponse = {
  account_id: string;
  name: string;
  owner_user_id: string;
  base_currency: string;
  sim_mode: boolean;
  hedge_auto: boolean;
  active: boolean;
  kraken_status?: {
    status?: string;
    last_rotated_at?: string | null;
  } | null;
};

const normalizeInput = (value: string) => value.trim();

const AccountProvisioner: React.FC = () => {
  const { readOnly, accessToken, mfaContext, mfaVerified, claims } =
    useAuthClaims();
  const [accounts, setAccounts] = useState<AccountSummary[]>([]);
  const [accountsLoading, setAccountsLoading] = useState<boolean>(false);
  const [accountsError, setAccountsError] = useState<string | null>(null);
  const [formState, setFormState] = useState<AccountCreatePayload>({
    name: "",
    owner_user_id: "",
    base_currency: "USD",
    sim_mode: false,
    hedge_auto: true,
    initial_api_key: "",
    initial_api_secret: "",
  });
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitSuccess, setSubmitSuccess] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

  const authorized = useMemo(() => !readOnly && Boolean(accessToken), [
    readOnly,
    accessToken,
  ]);

  const loadAccounts = useCallback(
    async (signal: AbortSignal) => {
      if (!authorized) {
        setAccounts([]);
        setAccountsError(null);
        return;
      }

      try {
        setAccountsLoading(true);
        setAccountsError(null);
        const response = await fetch("/accounts/list", {
          method: "GET",
          headers: {
            Accept: "application/json",
            ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
          },
          signal,
        });

        if (!response.ok) {
          throw new Error(`Failed to load accounts: ${response.status}`);
        }

        const payload = (await response.json()) as AccountSummary[];
        setAccounts(payload);
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        console.error("Failed to load account list", error);
        setAccountsError("Unable to load account list. Try refreshing later.");
      } finally {
        if (!signal.aborted) {
          setAccountsLoading(false);
        }
      }
    },
    [accessToken, authorized]
  );

  useEffect(() => {
    const controller = new AbortController();
    loadAccounts(controller.signal).catch((error) => {
      if (!controller.signal.aborted) {
        console.error("Failed to initialize account list", error);
      }
    });

    return () => controller.abort();
  }, [loadAccounts]);

  const handleInputChange = (
    field: keyof AccountCreatePayload,
    value: string | boolean
  ) => {
    setFormState((previous) => ({
      ...previous,
      [field]: value,
    }));
  };

  const resetForm = useCallback(() => {
    setFormState((previous) => ({
      ...previous,
      name: "",
      owner_user_id: "",
      initial_api_key: "",
      initial_api_secret: "",
    }));
  }, []);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (readOnly) {
      setSubmitError("Your session is read-only. Account provisioning is disabled.");
      setSubmitSuccess(null);
      return;
    }

    if (!accessToken) {
      setSubmitError("Authenticate with SSO before creating accounts.");
      setSubmitSuccess(null);
      return;
    }

    if (!mfaVerified) {
      setSubmitError("Multi-factor authentication is required before provisioning.");
      setSubmitSuccess(null);
      return;
    }

    const trimmedName = normalizeInput(formState.name);
    const trimmedOwner = normalizeInput(formState.owner_user_id);

    if (!trimmedName) {
      setSubmitError("Account name is required.");
      setSubmitSuccess(null);
      return;
    }

    if (!trimmedOwner) {
      setSubmitError("Owner user ID is required.");
      setSubmitSuccess(null);
      return;
    }

    setSubmitError(null);
    setSubmitSuccess(null);
    setIsSubmitting(true);

    const payload: AccountCreatePayload = {
      name: trimmedName,
      owner_user_id: trimmedOwner,
      base_currency: normalizeInput(formState.base_currency) || "USD",
      sim_mode: Boolean(formState.sim_mode),
      hedge_auto: Boolean(formState.hedge_auto),
    };

    const key = normalizeInput(formState.initial_api_key ?? "");
    const secret = normalizeInput(formState.initial_api_secret ?? "");
    if (key) {
      payload.initial_api_key = key;
    }
    if (secret) {
      payload.initial_api_secret = secret;
    }

    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Accept: "application/json",
        "X-MFA-Context": mfaContext,
      };
      if (accessToken) {
        headers.Authorization = `Bearer ${accessToken}`;
      }
      const userIdCandidate =
        (claims?.sub as string | undefined) ??
        (claims?.user_id as string | undefined) ??
        (claims?.userId as string | undefined);
      if (userIdCandidate && typeof userIdCandidate === "string") {
        headers["X-User-Id"] = userIdCandidate;
      }
      const roleCandidate =
        typeof claims?.role === "string" ? claims.role : undefined;
      if (roleCandidate) {
        headers["X-User-Role"] = roleCandidate;
      }
      const accountScopes = claims?.account_scopes;
      if (Array.isArray(accountScopes) && accountScopes.length > 0) {
        headers["X-Account-Ids"] = accountScopes.join(",");
      } else if (typeof accountScopes === "string" && accountScopes.trim()) {
        headers["X-Account-Ids"] = accountScopes.trim();
      }

      const response = await fetch("/accounts/create", {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || `Failed with status ${response.status}`);
      }

      const body = (await response.json()) as AccountCreateResponse;
      const rotationTs = body.kraken_status?.last_rotated_at
        ? formatLondonTime(body.kraken_status.last_rotated_at)
        : null;
      setSubmitSuccess(
        rotationTs
          ? `Account ${body.name} created. Kraken credentials rotated at ${rotationTs}.`
          : `Account ${body.name} created successfully.`
      );
      resetForm();
      const controller = new AbortController();
      try {
        await loadAccounts(controller.signal);
      } finally {
        controller.abort();
      }
    } catch (error) {
      console.error("Failed to create account", error);
      setSubmitError(
        error instanceof Error ? error.message : "Failed to create account."
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  const sortedAccounts = useMemo(() => {
    return [...accounts].sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: "base" })
    );
  }, [accounts]);

  return (
    <section className="space-y-6">
      <div className="rounded-lg border border-slate-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">
          Provision Trading Account
        </h2>
        <p className="mt-1 text-sm text-slate-600">
          Create isolated company and director accounts. Kraken API credentials can be
          supplied during creation and are stored securely per account.
        </p>

        <form className="mt-4 space-y-4" onSubmit={handleSubmit}>
          <div className="grid gap-4 md:grid-cols-2">
            <label className="flex flex-col text-sm font-medium text-slate-700">
              Account Name
              <input
                className="mt-1 rounded-md border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                type="text"
                value={formState.name}
                onChange={(event) => handleInputChange("name", event.target.value)}
                placeholder="Director 1"
                required
              />
            </label>
            <label className="flex flex-col text-sm font-medium text-slate-700">
              Owner User ID
              <input
                className="mt-1 rounded-md border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                type="text"
                value={formState.owner_user_id}
                onChange={(event) =>
                  handleInputChange("owner_user_id", event.target.value)
                }
                placeholder="UUID of responsible director"
                required
              />
            </label>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            <label className="flex flex-col text-sm font-medium text-slate-700">
              Base Currency
              <input
                className="mt-1 rounded-md border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                type="text"
                value={formState.base_currency}
                onChange={(event) =>
                  handleInputChange("base_currency", event.target.value.toUpperCase())
                }
                placeholder="USD"
              />
            </label>
            <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
                checked={formState.hedge_auto}
                onChange={(event) => handleInputChange("hedge_auto", event.target.checked)}
              />
              Enable Auto Hedge
            </label>
            <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
                checked={formState.sim_mode}
                onChange={(event) => handleInputChange("sim_mode", event.target.checked)}
              />
              Start in Simulation Mode
            </label>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <label className="flex flex-col text-sm font-medium text-slate-700">
              Kraken API Key (optional)
              <input
                className="mt-1 rounded-md border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                type="password"
                value={formState.initial_api_key}
                onChange={(event) =>
                  handleInputChange("initial_api_key", event.target.value)
                }
                placeholder="Provide to upload during creation"
              />
            </label>
            <label className="flex flex-col text-sm font-medium text-slate-700">
              Kraken API Secret (optional)
              <input
                className="mt-1 rounded-md border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                type="password"
                value={formState.initial_api_secret}
                onChange={(event) =>
                  handleInputChange("initial_api_secret", event.target.value)
                }
                placeholder="Stored in account-specific secret"
              />
            </label>
          </div>

          {submitError ? (
            <p className="text-sm text-red-600">{submitError}</p>
          ) : null}
          {submitSuccess ? (
            <p className="text-sm text-green-600">{submitSuccess}</p>
          ) : null}

          <button
            type="submit"
            className="inline-flex items-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm transition hover:bg-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-60"
            disabled={isSubmitting || readOnly}
          >
            {isSubmitting ? "Provisioning…" : "Create Account"}
          </button>
        </form>
      </div>

      <div className="rounded-lg border border-slate-200 bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between">
          <h3 className="text-base font-semibold text-slate-900">Existing Accounts</h3>
          <button
            type="button"
            className="inline-flex items-center rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 shadow-sm transition hover:bg-slate-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-60"
            onClick={() => {
              const controller = new AbortController();
              loadAccounts(controller.signal)
                .catch((error) => {
                  if (!controller.signal.aborted) {
                    console.error("Failed to refresh account list", error);
                    setAccountsError(
                      "Unable to refresh accounts. Check your connection and retry."
                    );
                  }
                })
                .finally(() => controller.abort());
            }}
            disabled={accountsLoading}
          >
            {accountsLoading ? "Refreshing…" : "Refresh"}
          </button>
        </div>
        {accountsError ? (
          <p className="mt-3 text-sm text-red-600">{accountsError}</p>
        ) : null}
        {sortedAccounts.length === 0 && !accountsError ? (
          <p className="mt-3 text-sm text-slate-600">
            No accounts have been provisioned yet. Create a company account and both
            director accounts to enable trading.
          </p>
        ) : null}
        {sortedAccounts.length > 0 ? (
          <div className="mt-4 overflow-x-auto">
            <table className="min-w-full divide-y divide-slate-200 text-sm">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-3 py-2 text-left font-semibold text-slate-700">
                    Name
                  </th>
                  <th className="px-3 py-2 text-left font-semibold text-slate-700">
                    Account ID
                  </th>
                  <th className="px-3 py-2 text-left font-semibold text-slate-700">
                    Owner
                  </th>
                  <th className="px-3 py-2 text-left font-semibold text-slate-700">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {sortedAccounts.map((account) => (
                  <tr key={account.account_id}>
                    <td className="px-3 py-2 font-medium text-slate-900">
                      {account.name}
                    </td>
                    <td className="px-3 py-2 text-slate-600">
                      <code className="break-all text-xs">{account.account_id}</code>
                    </td>
                    <td className="px-3 py-2 text-slate-600">
                      <code className="break-all text-xs">{account.owner_user_id}</code>
                    </td>
                    <td className="px-3 py-2 text-slate-600">
                      {account.active ? "Active" : "Inactive"}
                      {account.sim_mode ? " · Simulation" : ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </div>
    </section>
  );
};

export default withErrorBoundary(AccountProvisioner, {
  componentName: "Account Provisioner",
});
