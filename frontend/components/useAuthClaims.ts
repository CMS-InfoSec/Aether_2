import { useEffect, useMemo, useState } from "react";

export interface AuthClaims {
  role?: string;
  permissions?: string[];
  read_only?: boolean;
  [key: string]: unknown;
}

declare global {
  interface Window {
    __AETHER_CLAIMS__?: AuthClaims;
    __AETHER_ACCESS_TOKEN__?: string;
  }
}

const decodeJwtClaims = (token: string): AuthClaims | null => {
  const segments = token.split(".");
  if (segments.length < 2) {
    return null;
  }
  const payload = segments[1];
  try {
    const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(normalized.length + ((4 - (normalized.length % 4)) % 4), "=");
    if (typeof atob !== "function") {
      console.warn("Base64 decoder not available in this environment");
      return null;
    }
    const decoded = atob(padded);
    const parsed = JSON.parse(decoded) as AuthClaims;
    return parsed;
  } catch (error) {
    console.warn("Failed to decode JWT payload", error);
    return null;
  }
};

const safeGetStorageItem = (
  storageType: "localStorage" | "sessionStorage",
  key: string
): string | null => {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    const storage = window[storageType];
    if (!storage) {
      return null;
    }
    return storage.getItem(key);
  } catch (error) {
    console.warn(`Unable to access ${storageType}`, error);
    return null;
  }
};

const readClaimsFromEnvironment = (): AuthClaims | null => {
  if (typeof window === "undefined") {
    return null;
  }

  if (window.__AETHER_CLAIMS__) {
    return window.__AETHER_CLAIMS__;
  }

  const tokenCandidates: (string | null | undefined)[] = [
    window.__AETHER_ACCESS_TOKEN__,
    safeGetStorageItem("sessionStorage", "aether.access_token"),
    safeGetStorageItem("sessionStorage", "access_token"),
    safeGetStorageItem("localStorage", "aether.access_token"),
    safeGetStorageItem("localStorage", "access_token"),
  ];

  for (const token of tokenCandidates) {
    if (token) {
      const claims = decodeJwtClaims(token);
      if (claims) {
        return claims;
      }
    }
  }

  return null;
};

export const useAuthClaims = () => {
  const [claims, setClaims] = useState<AuthClaims | null>(() =>
    readClaimsFromEnvironment()
  );

  useEffect(() => {
    if (typeof window === "undefined") {
      return undefined;
    }

    const syncClaims = () => {
      setClaims(readClaimsFromEnvironment());
    };

    window.addEventListener("storage", syncClaims);
    const interval = window.setInterval(syncClaims, 5000);

    return () => {
      window.removeEventListener("storage", syncClaims);
      window.clearInterval(interval);
    };
  }, []);

  const permissions = useMemo(() => {
    if (!claims?.permissions) {
      return [] as string[];
    }
    return Array.from(new Set(claims.permissions)).sort();
  }, [claims]);

  const readOnly = useMemo(() => {
    if (!claims) {
      return false;
    }
    if (typeof claims.read_only === "boolean") {
      return claims.read_only;
    }
    const role = typeof claims.role === "string" ? claims.role.toLowerCase() : "";
    return role === "auditor";
  }, [claims]);

  return { claims, permissions, readOnly } as const;
};

export default useAuthClaims;
