import React, { type ReactNode } from "react";

import { ApiError } from "./apiClient";

export interface ErrorBoundaryProps {
  children: ReactNode;
  fallbackTitle?: string;
  supportContact?: string;
  autoRetry?: boolean;
  maxAutoRetries?: number;
  retryDelayMs?: number;
  onReset?: () => void;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
  retryCount: number;
}

const DEFAULT_SUPPORT_CONTACT = "support@aether.local";

export class ErrorBoundary extends React.Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  static defaultProps: Partial<ErrorBoundaryProps> = {
    fallbackTitle: "We hit a snag",
    supportContact: DEFAULT_SUPPORT_CONTACT,
    autoRetry: true,
    maxAutoRetries: 2,
    retryDelayMs: 3000,
  };

  state: ErrorBoundaryState = {
    hasError: false,
    error: null,
    retryCount: 0,
  };

  private retryHandle: number | null = null;

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    // Surface errors to the console for observability while keeping the UI friendly.
    console.error("Error boundary captured an exception", error, errorInfo);
  }

  componentDidUpdate(
    _prevProps: ErrorBoundaryProps,
    prevState: ErrorBoundaryState
  ): void {
    if (!this.state.hasError && prevState.hasError) {
      this.clearRetryTimer();
      return;
    }

    if (this.state.error !== prevState.error && this.state.hasError) {
      this.maybeScheduleAutoRetry();
    }
  }

  componentWillUnmount(): void {
    this.clearRetryTimer();
  }

  private clearRetryTimer() {
    if (this.retryHandle !== null) {
      if (typeof window !== "undefined") {
        window.clearTimeout(this.retryHandle);
      }
      this.retryHandle = null;
    }
  }

  private isTransientError(error: Error | null): boolean {
    if (!error) {
      return false;
    }

    if (error instanceof ApiError) {
      if (error.transient || error.isTimeout) {
        return true;
      }
      if (typeof error.status === "number" && error.status >= 500) {
        return true;
      }
      return false;
    }

    const message = error.message.toLowerCase();
    return message.includes("timeout") || message.includes("network");
  }

  private maybeScheduleAutoRetry() {
    const { autoRetry, maxAutoRetries = 0, retryDelayMs = 0 } = this.props;
    if (!autoRetry || typeof window === "undefined") {
      return;
    }

    if (!this.isTransientError(this.state.error)) {
      return;
    }

    if (this.state.retryCount >= maxAutoRetries) {
      return;
    }

    if (this.retryHandle !== null) {
      return;
    }

    const delay = Math.max(retryDelayMs, 0);
    this.retryHandle = window.setTimeout(() => this.handleRetry(true), delay);
  }

  private handleRetry = (auto: boolean) => {
    this.clearRetryTimer();
    this.setState((prev) => ({
      hasError: false,
      error: null,
      retryCount: auto ? prev.retryCount + 1 : 0,
    }));
    this.props.onReset?.();
  };

  private renderDetails(error: Error): ReactNode {
    if (error instanceof ApiError && error.details) {
      return (
        <pre className="max-w-xl overflow-x-auto rounded bg-slate-950/60 p-3 text-left text-xs text-amber-200/70">
          {error.details}
        </pre>
      );
    }

    return (
      <pre className="max-w-xl overflow-x-auto rounded bg-slate-950/60 p-3 text-left text-xs text-amber-200/70">
        {error.message}
      </pre>
    );
  }

  render(): ReactNode {
    if (!this.state.hasError || !this.state.error) {
      return this.props.children;
    }

    const {
      fallbackTitle = ErrorBoundary.defaultProps.fallbackTitle!,
      supportContact = ErrorBoundary.defaultProps.supportContact!,
      autoRetry = ErrorBoundary.defaultProps.autoRetry!,
      retryDelayMs = ErrorBoundary.defaultProps.retryDelayMs!,
      maxAutoRetries = ErrorBoundary.defaultProps.maxAutoRetries!,
    } = this.props;

    const transient = this.isTransientError(this.state.error);
    const autoRetryEnabled =
      autoRetry &&
      transient &&
      this.state.retryCount < maxAutoRetries &&
      typeof window !== "undefined";

    const nextRetrySeconds = Math.max(Math.round(retryDelayMs / 1000), 1);

    return (
      <div className="flex min-h-[50vh] flex-col items-center justify-center gap-4 rounded-lg border border-amber-400/30 bg-amber-500/10 p-6 text-center text-amber-100">
        <div className="text-xl font-semibold text-amber-200">{fallbackTitle}</div>
        <p className="max-w-xl text-sm text-amber-100/80">
          {autoRetryEnabled
            ? `We’re having trouble talking to the Aether services. We’ll retry automatically in ${nextRetrySeconds} seconds.`
            : "We’re having trouble talking to the Aether services right now. Please try again or reach out if the issue persists."}
        </p>
        {this.renderDetails(this.state.error)}
        <div className="flex flex-wrap items-center justify-center gap-3 text-sm">
          <button
            type="button"
            onClick={() => this.handleRetry(false)}
            className="rounded-md bg-amber-400 px-4 py-2 font-semibold text-slate-900 shadow-sm transition hover:bg-amber-300 focus:outline-none focus:ring-2 focus:ring-amber-200"
          >
            Try again now
          </button>
          <span className="text-xs text-amber-100/70">
            Need help? Contact {" "}
            <a className="underline" href={`mailto:${supportContact}`}>
              {supportContact}
            </a>
            .
          </span>
        </div>
      </div>
    );
  }
}

export default ErrorBoundary;
