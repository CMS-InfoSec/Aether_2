import React from "react";

import ErrorBoundary, { type ErrorBoundaryProps } from "./ErrorBoundary";
import { ensureFetchInterceptor } from "./apiClient";

ensureFetchInterceptor();

export interface WithErrorBoundaryOptions extends Partial<ErrorBoundaryProps> {
  componentName?: string;
}

export function withErrorBoundary<P>(
  Component: React.ComponentType<P>,
  options?: WithErrorBoundaryOptions
): React.ComponentType<P> {
  const { componentName, ...boundaryOptions } = options ?? {};
  const displayName = componentName ?? Component.displayName ?? Component.name ?? "Component";
  const fallbackTitle =
    boundaryOptions.fallbackTitle ?? `${displayName} is temporarily unavailable`;
  const boundaryProps: ErrorBoundaryProps = {
    ...boundaryOptions,
    fallbackTitle,
  };

  const Wrapped: React.FC<P> = (props) => (
    <ErrorBoundary {...boundaryProps}>
      <Component {...props} />
    </ErrorBoundary>
  );

  Wrapped.displayName = `WithErrorBoundary(${displayName})`;

  return Wrapped;
}

export default withErrorBoundary;
