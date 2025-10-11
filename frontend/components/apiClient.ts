const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_MAX_RETRIES = 2;
const DEFAULT_RETRY_DELAY_MS = 500;
const DEFAULT_RETRY_BACKOFF_FACTOR = 2;
const DEFAULT_TRANSIENT_STATUS_CODES = new Set([408, 425, 429, 500, 502, 503, 504]);

export interface ApiFetchSettings extends RequestInit {
  timeoutMs?: number;
  maxRetries?: number;
  retryDelayMs?: number;
  retryBackoffFactor?: number;
  transientStatusCodes?: number[];
}

export interface ApiErrorOptions {
  status?: number;
  response?: Response;
  transient?: boolean;
  isTimeout?: boolean;
  details?: string;
  cause?: unknown;
}

export class ApiError extends Error {
  public readonly status?: number;
  public readonly response?: Response;
  public readonly transient: boolean;
  public readonly isTimeout: boolean;
  public readonly details?: string;

  constructor(message: string, options: ApiErrorOptions = {}) {
    super(message);
    this.name = "ApiError";
    this.status = options.status;
    this.response = options.response;
    this.transient = options.transient ?? false;
    this.isTimeout = options.isTimeout ?? false;
    this.details = options.details;
    if (options.cause !== undefined) {
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (this as any).cause = options.cause;
      } catch {
        // Ignore failures assigning cause; best effort only.
      }
    }
  }
}

const delay = (ms: number) =>
  new Promise<void>((resolve) => {
    if (ms <= 0) {
      resolve();
      return;
    }
    setTimeout(resolve, ms);
  });

const isAbortError = (error: unknown): boolean => {
  if (!error) {
    return false;
  }

  if (typeof DOMException !== "undefined" && error instanceof DOMException) {
    return error.name === "AbortError";
  }

  return Boolean((error as { name?: string }).name === "AbortError");
};

const isNetworkError = (error: unknown): boolean =>
  typeof error === "object" && error instanceof TypeError;

const extractErrorDetails = async (response: Response): Promise<string | undefined> => {
  const cloned = response.clone();
  const contentType = cloned.headers.get("content-type")?.toLowerCase() ?? "";

  try {
    if (contentType.includes("application/json")) {
      const payload = (await cloned.json()) as Record<string, unknown>;
      const detailFields = ["detail", "message", "error", "description"] as const;
      for (const field of detailFields) {
        const value = payload[field];
        if (typeof value === "string" && value.trim().length) {
          return value.trim();
        }
      }
      const fallback = JSON.stringify(payload);
      if (fallback !== "{}") {
        return fallback;
      }
    } else {
      const text = await cloned.text();
      if (text && text.trim().length) {
        return text.trim();
      }
    }
  } catch {
    // Ignore parsing failures and fall through to the generic message.
  }

  return undefined;
};

const formatServerErrorMessage = async (
  response: Response,
  isTransient: boolean
): Promise<{ message: string; details?: string }> => {
  const details = await extractErrorDetails(response);
  const statusLabel = response.statusText || `status ${response.status}`;
  const baseMessage = isTransient
    ? "We\u2019re experiencing a temporary issue reaching the service."
    : "The service encountered an unexpected error.";
  const message = `${baseMessage} (${statusLabel}). Please try again.`;
  return { message, details };
};

let interceptorInstalled = false;

export const ensureFetchInterceptor = () => {
  if (interceptorInstalled) {
    return;
  }

  if (typeof globalThis.fetch !== "function") {
    return;
  }

  const originalFetch = globalThis.fetch.bind(globalThis);

  const wrappedFetch: typeof fetch = async (
    input: RequestInfo | URL,
    initArg?: ApiFetchSettings
  ): Promise<Response> => {
    const init = { ...(initArg ?? {}) } as ApiFetchSettings;
    const {
      timeoutMs = DEFAULT_TIMEOUT_MS,
      maxRetries = DEFAULT_MAX_RETRIES,
      retryDelayMs = DEFAULT_RETRY_DELAY_MS,
      retryBackoffFactor = DEFAULT_RETRY_BACKOFF_FACTOR,
      transientStatusCodes,
    } = init;

    const retryableStatuses = new Set(
      (transientStatusCodes ?? Array.from(DEFAULT_TRANSIENT_STATUS_CODES)).map((code) => Number(code))
    );

    delete init.timeoutMs;
    delete init.maxRetries;
    delete init.retryDelayMs;
    delete init.retryBackoffFactor;
    delete init.transientStatusCodes;

    const requestInit = { ...init } as RequestInit;

    for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
      const isLastAttempt = attempt === maxRetries;
      const controller = new AbortController();
      const externalSignal = initArg?.signal;
      let externalAbort = false;
      let externalAbortHandler: (() => void) | undefined;

      if (externalSignal) {
        if (externalSignal.aborted) {
          externalAbort = true;
          controller.abort();
        } else {
          externalAbortHandler = () => {
            externalAbort = true;
            controller.abort();
          };
          externalSignal.addEventListener("abort", externalAbortHandler);
        }
      }

      const timeoutId =
        timeoutMs > 0 && typeof window !== "undefined"
          ? window.setTimeout(() => controller.abort(), timeoutMs)
          : undefined;

      try {
        const response = await originalFetch(input, {
          ...requestInit,
          signal: controller.signal,
        });

        if (timeoutId !== undefined) {
          window.clearTimeout(timeoutId);
        }
        if (externalSignal && externalAbortHandler) {
          externalSignal.removeEventListener("abort", externalAbortHandler);
        }

        if (retryableStatuses.has(response.status)) {
          if (!isLastAttempt) {
            const backoff = retryDelayMs * Math.pow(retryBackoffFactor, attempt);
            await delay(backoff);
            continue;
          }

          const { message, details } = await formatServerErrorMessage(
            response,
            true
          );
          throw new ApiError(message, {
            status: response.status,
            response,
            transient: true,
            details,
          });
        }

        if (!response.ok && response.status >= 500) {
          const { message, details } = await formatServerErrorMessage(
            response,
            false
          );
          throw new ApiError(message, {
            status: response.status,
            response,
            transient: false,
            details,
          });
        }

        return response;
      } catch (error) {
        if (timeoutId !== undefined) {
          window.clearTimeout(timeoutId);
        }
        if (externalSignal && externalAbortHandler) {
          externalSignal.removeEventListener("abort", externalAbortHandler);
        }

        const aborted = isAbortError(error);
        if (externalAbort && aborted) {
          throw error;
        }

        const timedOut = aborted && !externalAbort;
        const networkIssue = isNetworkError(error);
        const transientFailure = timedOut || networkIssue;

        if (transientFailure && !isLastAttempt) {
          const backoff = retryDelayMs * Math.pow(retryBackoffFactor, attempt);
          await delay(backoff);
          continue;
        }

        if (error instanceof ApiError) {
          throw error;
        }

        const message = timedOut
          ? "The request timed out before the server responded. Please try again."
          : "We couldn\u2019t reach the service. Check your connection and try again.";

        throw new ApiError(message, {
          transient: transientFailure,
          isTimeout: timedOut,
          cause: error,
        });
      }
    }

    throw new ApiError("Request failed after retries.");
  };

  globalThis.fetch = wrappedFetch;
  interceptorInstalled = true;
};

if (typeof window !== "undefined") {
  ensureFetchInterceptor();
}
