import React, {
  FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { formatLondonTime } from "./timezone";
import { useAuthClaims } from "./useAuthClaims";

type TradeSide = "buy" | "sell";

type SyntheticIntentFormState = {
  symbol: string;
  side: TradeSide;
  size: string;
  type: string;
  correlationId: string;
};

type KafkaMessage = {
  topic: string;
  partition?: number;
  offset?: number;
  timestamp?: string;
  key?: string;
  correlationId?: string;
  value: unknown;
};

type ChaosToggle = "safe_mode" | "latency";

type AuditEntry = {
  id?: string;
  actor: string;
  action: string;
  entity: string;
  ts: string;
  details?: string | null;
};

const DEFAULT_INTENT_STATE: SyntheticIntentFormState = {
  symbol: "BTC-USD",
  side: "buy",
  size: "1",
  type: "market",
  correlationId: "sandbox-" + Date.now().toString(36),
};

const KAFKA_TOPICS: KafkaMessage["topic"][] = [
  "intents",
  "decisions",
  "orders",
  "fills",
];

const detectEnvironment = (): string => {
  if (typeof window === "undefined") {
    return "unknown";
  }

  const globalCandidates: unknown[] = [
    (window as Record<string, unknown>).__AETHER_ENVIRONMENT__,
    (window as Record<string, unknown>).__AETHER_ENV__,
    (window as Record<string, unknown>).__ENV__,
  ];

  for (const candidate of globalCandidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim().toLowerCase();
    }
  }

  const metaCandidates = [
    document.querySelector("meta[name='environment']"),
    document.querySelector("meta[name='aether-environment']"),
    document.querySelector("meta[name='app-environment']"),
  ];

  for (const meta of metaCandidates) {
    const content = meta?.getAttribute("content");
    if (content && content.trim()) {
      return content.trim().toLowerCase();
    }
  }

  const hostname = window.location?.hostname ?? "";
  if (hostname.toLowerCase().includes("staging")) {
    return "staging";
  }

  return "unknown";
};

const ensureString = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch (error) {
    return String(value);
  }
};

const ensureNumber = (value: unknown): number | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value.trim());
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return undefined;
};

const parseKafkaMessages = (
  payload: unknown,
  fallbackTopic: string
): KafkaMessage[] => {
  const toMessage = (raw: unknown): KafkaMessage | null => {
    if (!raw) {
      return null;
    }
    if (typeof raw === "string") {
      return {
        topic: fallbackTopic,
        value: raw,
      };
    }
    if (typeof raw !== "object") {
      return null;
    }
    const record = raw as Record<string, unknown>;
    const topic = ensureString(record.topic || fallbackTopic) || fallbackTopic;
    const key = ensureString(record.key ?? "");
    const timestamp = ensureString(
      record.timestamp ?? record.ts ?? record.time ?? ""
    );
    const offset = ensureNumber(record.offset ?? record.seq ?? record.sequence);
    const partition = ensureNumber(record.partition ?? record.part);

    const valueRaw = record.value ?? record.payload ?? record.message ?? null;
    let parsedValue: unknown = valueRaw;
    if (typeof valueRaw === "string") {
      const trimmed = valueRaw.trim();
      if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
        try {
          parsedValue = JSON.parse(trimmed);
        } catch (error) {
          parsedValue = valueRaw;
        }
      }
    }

    const correlationIdCandidate =
      record.correlation_id ??
      record.correlationId ??
      (typeof parsedValue === "object" && parsedValue
        ? (parsedValue as Record<string, unknown>).correlation_id ??
          (parsedValue as Record<string, unknown>).correlationId
        : undefined);

    const correlationId = ensureString(correlationIdCandidate ?? "");

    return {
      topic,
      key: key || undefined,
      timestamp: timestamp || undefined,
      offset,
      partition,
      correlationId: correlationId || undefined,
      value: parsedValue ?? valueRaw,
    };
  };

  const extractArray = (value: unknown): unknown[] => {
    if (!value) {
      return [];
    }
    if (Array.isArray(value)) {
      return value;
    }
    if (typeof value === "object") {
      const container = value as Record<string, unknown>;
      const candidateKeys = [
        "messages",
        "records",
        "entries",
        "items",
        "results",
        "data",
        "payload",
      ];
      for (const key of candidateKeys) {
        if (Array.isArray(container[key])) {
          return container[key] as unknown[];
        }
      }
    }
    return [];
  };

  return extractArray(payload)
    .map((item) => toMessage(item))
    .filter((item): item is KafkaMessage => item !== null);
};

const parseAuditEntries = (payload: unknown): AuditEntry[] => {
  const mapEntry = (raw: unknown): AuditEntry | null => {
    if (!raw || typeof raw !== "object") {
      return null;
    }
    const record = raw as Record<string, unknown>;
    const actor = ensureString(
      record.actor ?? record.actor_id ?? record.user ?? record.principal ?? ""
    );
    const action = ensureString(record.action ?? record.event ?? record.type ?? "");
    const entity = ensureString(
      record.entity ??
        record.entity_id ??
        record.target ??
        record.resource ??
        record.subject ??
        record.object ??
        ""
    );
    const ts = ensureString(
      record.ts ?? record.timestamp ?? record.created_at ?? record.time ?? ""
    );
    const idValue = record.id ?? record.event_id ?? record.audit_id ?? null;
    const id = ensureString(idValue ?? "");
    const detailsCandidate = record.details ?? record.metadata ?? record.payload ?? null;
    let details: string | null = null;
    if (typeof detailsCandidate === "string") {
      details = detailsCandidate;
    } else if (detailsCandidate) {
      try {
        details = JSON.stringify(detailsCandidate, null, 2);
      } catch (error) {
        details = ensureString(detailsCandidate);
      }
    }

    if (!actor && !action && !entity && !ts) {
      return null;
    }

    return {
      id: id || undefined,
      actor,
      action,
      entity: entity || "—",
      ts,
      details,
    };
  };

  const asArray = (value: unknown): unknown[] => {
    if (!value) {
      return [];
    }
    if (Array.isArray(value)) {
      return value;
    }
    if (typeof value === "object") {
      const container = value as Record<string, unknown>;
      const candidateKeys = ["logs", "entries", "results", "data"] as const;
      for (const key of candidateKeys) {
        if (Array.isArray(container[key])) {
          return container[key] as unknown[];
        }
      }
    }
    return [];
  };

  return asArray(payload)
    .map((item) => mapEntry(item))
    .filter((item): item is AuditEntry => item !== null)
    .sort((a, b) => b.ts.localeCompare(a.ts))
    .slice(0, 100);
};

const formatTimestamp = (value?: string) => {
  if (!value) {
    return "—";
  }
  const formatted = formatLondonTime(value);
  if (!formatted) {
    return value;
  }
  return formatted;
};

const DevSandbox: React.FC = () => {
  const { claims } = useAuthClaims();
  const [environment, setEnvironment] = useState<string>(() => detectEnvironment());

  useEffect(() => {
    setEnvironment(detectEnvironment());
  }, []);

  const role = useMemo(
    () => (claims?.role ? String(claims.role).toLowerCase() : ""),
    [claims]
  );
  const permissions = useMemo(() => {
    if (!claims?.permissions) {
      return new Set<string>();
    }
    return new Set(
      claims.permissions
        .map((permission) => String(permission).toLowerCase())
        .filter(Boolean)
    );
  }, [claims]);

  const isAdmin = role === "admin" || permissions.has("admin") || permissions.has("sandbox");
  const isStaging = environment === "staging" || environment === "stage";

  if (!isAdmin || !isStaging) {
    return null;
  }

  const [intentState, setIntentState] = useState<SyntheticIntentFormState>(
    DEFAULT_INTENT_STATE
  );
  const [intentLoading, setIntentLoading] = useState(false);
  const [intentError, setIntentError] = useState<string | null>(null);
  const [intentResult, setIntentResult] = useState<string | null>(null);

  const handleIntentChange = (
    field: keyof SyntheticIntentFormState,
    value: string
  ) => {
    setIntentState((prev) => ({
      ...prev,
      [field]: value,
    }));
  };

  const submitSyntheticIntent = async (
    event: FormEvent<HTMLFormElement>
  ) => {
    event.preventDefault();
    if (!intentState.symbol.trim()) {
      setIntentError("Symbol is required to submit an intent.");
      setIntentResult(null);
      return;
    }
    const sizeValue = Number(intentState.size);
    if (!Number.isFinite(sizeValue) || sizeValue <= 0) {
      setIntentError("Size must be a positive number.");
      setIntentResult(null);
      return;
    }

    setIntentLoading(true);
    setIntentError(null);
    setIntentResult(null);

    const payload = {
      intent: {
        symbol: intentState.symbol.trim(),
        side: intentState.side,
        size: sizeValue,
        type: intentState.type.trim() || "synthetic",
        correlation_id:
          intentState.correlationId.trim() ||
          `sandbox-${Date.now().toString(36)}`,
        sandbox: true,
        injected_at: new Date().toISOString(),
      },
    };

    try {
      const response = await fetch("/sequencer/submit_intent?sandbox=true", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });

      const text = await response.text();
      let message = text;
      if (response.headers.get("Content-Type")?.includes("application/json")) {
        try {
          const data = JSON.parse(text);
          message = JSON.stringify(data, null, 2);
        } catch (error) {
          message = text;
        }
      }

      if (!response.ok) {
        throw new Error(message || `Intent submission failed (${response.status}).`);
      }

      setIntentResult(message || "Intent submitted successfully.");
      setIntentError(null);
    } catch (cause) {
      console.error("Failed to submit sandbox intent", cause);
      setIntentResult(null);
      setIntentError(
        cause instanceof Error
          ? cause.message
          : "Unable to submit the synthetic intent."
      );
    } finally {
      setIntentLoading(false);
    }
  };

  const [selectedTopic, setSelectedTopic] = useState<KafkaMessage["topic"]>(
    KAFKA_TOPICS[0]
  );
  const [streamMessages, setStreamMessages] = useState<KafkaMessage[]>([]);
  const [streamError, setStreamError] = useState<string | null>(null);
  const [streamLoading, setStreamLoading] = useState<boolean>(false);
  const [correlationFilter, setCorrelationFilter] = useState<string>("");
  const [autoRefresh, setAutoRefresh] = useState<boolean>(true);
  const refreshTimerRef = useRef<number | null>(null);

  const fetchTopicMessages = useCallback(
    async (signal?: AbortSignal) => {
      setStreamLoading((prev) => prev || !streamMessages.length);
      try {
        const params = new URLSearchParams({ limit: "100" });
        if (correlationFilter.trim()) {
          params.set("correlation_id", correlationFilter.trim());
        }
        const response = await fetch(
          `/dev/kafka/topics/${encodeURIComponent(selectedTopic)}?${params.toString()}`,
          {
            method: "GET",
            headers: {
              Accept: "application/json",
            },
            signal,
          }
        );

        if (!response.ok) {
          const message = await response.text();
          throw new Error(
            message || `Failed to load topic '${selectedTopic}' (${response.status}).`
          );
        }

        const data = await response.json();
        const messages = parseKafkaMessages(data, selectedTopic);
        setStreamMessages(messages.slice(-100).reverse());
        setStreamError(null);
      } catch (cause) {
        if (signal?.aborted) {
          return;
        }
        console.error("Failed to fetch Kafka topic messages", cause);
        setStreamError(
          cause instanceof Error
            ? cause.message
            : "Unable to load Kafka topic messages."
        );
      } finally {
        if (!signal?.aborted) {
          setStreamLoading(false);
        }
      }
    },
    [selectedTopic, correlationFilter, streamMessages.length]
  );

  useEffect(() => {
    const controller = new AbortController();
    fetchTopicMessages(controller.signal).catch((cause) => {
      if (!controller.signal.aborted) {
        console.error("Initial Kafka message fetch failed", cause);
      }
    });
    return () => controller.abort();
  }, [fetchTopicMessages]);

  useEffect(() => {
    if (!autoRefresh) {
      if (refreshTimerRef.current) {
        window.clearInterval(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
      return undefined;
    }
    const interval = window.setInterval(() => {
      const controller = new AbortController();
      fetchTopicMessages(controller.signal).finally(() => {
        controller.abort();
      });
    }, 5000);
    refreshTimerRef.current = interval;
    return () => {
      window.clearInterval(interval);
      if (refreshTimerRef.current === interval) {
        refreshTimerRef.current = null;
      }
    };
  }, [autoRefresh, fetchTopicMessages]);

  const filteredMessages = useMemo(() => {
    if (!correlationFilter.trim()) {
      return streamMessages;
    }
    const normalized = correlationFilter.trim().toLowerCase();
    return streamMessages.filter((message) =>
      [
        message.correlationId,
        message.key,
        typeof message.value === "string" ? message.value : JSON.stringify(message.value),
      ]
        .filter(Boolean)
        .some((candidate) =>
          String(candidate).toLowerCase().includes(normalized)
        )
    );
  }, [streamMessages, correlationFilter]);

  const [chaosMessage, setChaosMessage] = useState<string | null>(null);
  const [chaosError, setChaosError] = useState<string | null>(null);
  const [chaosLoading, setChaosLoading] = useState<ChaosToggle | null>(null);
  const [safeModeChaos, setSafeModeChaos] = useState<boolean>(false);
  const [latencyChaos, setLatencyChaos] = useState<boolean>(false);

  const toggleChaos = async (toggle: ChaosToggle, desired: boolean) => {
    setChaosLoading(toggle);
    setChaosMessage(null);
    setChaosError(null);
    try {
      const response = await fetch(`/chaos/${toggle}`, {
        method: desired ? "POST" : "DELETE",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ enabled: desired }),
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(
          message || `Failed to toggle ${toggle.replace("_", " ")} (${response.status}).`
        );
      }

      const payload = await response.text();
      const statusMessage =
        payload && payload.trim().length
          ? payload
          : `${toggle.replace("_", " ")} ${desired ? "enabled" : "disabled"}.`;
      setChaosMessage(statusMessage);
      if (toggle === "safe_mode") {
        setSafeModeChaos(desired);
      } else {
        setLatencyChaos(desired);
      }
    } catch (cause) {
      console.error("Chaos toggle failed", cause);
      setChaosError(
        cause instanceof Error
          ? cause.message
          : "Unable to toggle chaos experiment."
      );
    } finally {
      setChaosLoading(null);
    }
  };

  const [auditEntries, setAuditEntries] = useState<AuditEntry[]>([]);
  const [auditError, setAuditError] = useState<string | null>(null);
  const [auditLoading, setAuditLoading] = useState<boolean>(false);
  const [auditQuery, setAuditQuery] = useState<string>("");

  const fetchAuditEntries = useCallback(async () => {
    setAuditLoading(true);
    setAuditError(null);
    try {
      const response = await fetch("/audit/query?limit=100", {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        const message = await response.text();
        throw new Error(
          message || `Failed to load audit entries (${response.status}).`
        );
      }
      const data = await response.json();
      setAuditEntries(parseAuditEntries(data));
    } catch (cause) {
      console.error("Failed to fetch audit entries", cause);
      setAuditError(
        cause instanceof Error
          ? cause.message
          : "Unable to load audit entries."
      );
      setAuditEntries([]);
    } finally {
      setAuditLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAuditEntries().catch((cause) => {
      console.error("Audit initialisation failed", cause);
    });
  }, [fetchAuditEntries]);

  const filteredAuditEntries = useMemo(() => {
    if (!auditQuery.trim()) {
      return auditEntries;
    }
    const normalized = auditQuery.trim().toLowerCase();
    return auditEntries.filter((entry) =>
      [entry.actor, entry.action, entry.entity, entry.ts, entry.details]
        .filter(Boolean)
        .some((value) => value!.toLowerCase().includes(normalized))
    );
  }, [auditEntries, auditQuery]);

  return (
    <section
      style={{
        border: "1px solid #ddd",
        borderRadius: "8px",
        padding: "24px",
        marginTop: "24px",
        backgroundColor: "#fafbff",
        boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
      }}
    >
      <header style={{ marginBottom: "16px" }}>
        <h2 style={{ margin: 0 }}>Developer Sandbox</h2>
        <p style={{ margin: "4px 0 0", color: "#555" }}>
          Staging-only utilities for administrators. Use responsibly.
        </p>
      </header>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
          gap: "24px",
        }}
      >
        <section
          style={{
            border: "1px solid #e2e8f0",
            borderRadius: "8px",
            padding: "16px",
            backgroundColor: "#fff",
          }}
        >
          <h3 style={{ marginTop: 0 }}>Synthetic Intent Injector</h3>
          <form onSubmit={submitSyntheticIntent}>
            <label style={{ display: "block", marginBottom: "8px" }}>
              <span>Symbol</span>
              <input
                type="text"
                value={intentState.symbol}
                onChange={(event) => handleIntentChange("symbol", event.target.value)}
                required
                style={{
                  width: "100%",
                  padding: "8px",
                  marginTop: "4px",
                  borderRadius: "4px",
                  border: "1px solid #cbd5f5",
                }}
              />
            </label>
            <label style={{ display: "block", marginBottom: "8px" }}>
              <span>Side</span>
              <select
                value={intentState.side}
                onChange={(event) =>
                  handleIntentChange("side", event.target.value as TradeSide)
                }
                style={{
                  width: "100%",
                  padding: "8px",
                  marginTop: "4px",
                  borderRadius: "4px",
                  border: "1px solid #cbd5f5",
                }}
              >
                <option value="buy">Buy</option>
                <option value="sell">Sell</option>
              </select>
            </label>
            <label style={{ display: "block", marginBottom: "8px" }}>
              <span>Size</span>
              <input
                type="number"
                min="0"
                step="any"
                value={intentState.size}
                onChange={(event) => handleIntentChange("size", event.target.value)}
                required
                style={{
                  width: "100%",
                  padding: "8px",
                  marginTop: "4px",
                  borderRadius: "4px",
                  border: "1px solid #cbd5f5",
                }}
              />
            </label>
            <label style={{ display: "block", marginBottom: "8px" }}>
              <span>Type</span>
              <input
                type="text"
                value={intentState.type}
                onChange={(event) => handleIntentChange("type", event.target.value)}
                style={{
                  width: "100%",
                  padding: "8px",
                  marginTop: "4px",
                  borderRadius: "4px",
                  border: "1px solid #cbd5f5",
                }}
              />
            </label>
            <label style={{ display: "block", marginBottom: "12px" }}>
              <span>Correlation ID</span>
              <input
                type="text"
                value={intentState.correlationId}
                onChange={(event) =>
                  handleIntentChange("correlationId", event.target.value)
                }
                style={{
                  width: "100%",
                  padding: "8px",
                  marginTop: "4px",
                  borderRadius: "4px",
                  border: "1px solid #cbd5f5",
                }}
              />
            </label>
            <button
              type="submit"
              disabled={intentLoading}
              style={{
                padding: "10px 16px",
                backgroundColor: intentLoading ? "#94a3b8" : "#2563eb",
                border: "none",
                color: "white",
                borderRadius: "4px",
                cursor: intentLoading ? "default" : "pointer",
              }}
            >
              {intentLoading ? "Submitting…" : "Submit Intent"}
            </button>
          </form>
          {intentError && (
            <p style={{ color: "#dc2626", marginTop: "12px" }}>{intentError}</p>
          )}
          {intentResult && (
            <pre
              style={{
                marginTop: "12px",
                padding: "12px",
                backgroundColor: "#f1f5f9",
                borderRadius: "4px",
                maxHeight: "200px",
                overflow: "auto",
              }}
            >
              {intentResult}
            </pre>
          )}
        </section>

        <section
          style={{
            border: "1px solid #e2e8f0",
            borderRadius: "8px",
            padding: "16px",
            backgroundColor: "#fff",
          }}
        >
          <h3 style={{ marginTop: 0 }}>Kafka Topic Stream</h3>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: "8px",
              marginBottom: "12px",
            }}
          >
            {KAFKA_TOPICS.map((topic) => (
              <button
                key={topic}
                type="button"
                onClick={() => setSelectedTopic(topic)}
                style={{
                  padding: "6px 12px",
                  borderRadius: "4px",
                  border: selectedTopic === topic ? "1px solid #2563eb" : "1px solid #cbd5f5",
                  backgroundColor:
                    selectedTopic === topic ? "rgba(37,99,235,0.1)" : "#fff",
                  cursor: "pointer",
                }}
              >
                {topic}
              </button>
            ))}
          </div>
          <label style={{ display: "block", marginBottom: "8px" }}>
            <span>Correlation ID filter</span>
            <input
              type="text"
              value={correlationFilter}
              onChange={(event) => setCorrelationFilter(event.target.value)}
              placeholder="Optional correlation identifier"
              style={{
                width: "100%",
                padding: "8px",
                marginTop: "4px",
                borderRadius: "4px",
                border: "1px solid #cbd5f5",
              }}
            />
          </label>
          <div
            style={{
              display: "flex",
              gap: "8px",
              alignItems: "center",
              marginBottom: "12px",
            }}
          >
            <button
              type="button"
              onClick={() => {
                void fetchTopicMessages();
              }}
              disabled={streamLoading}
              style={{
                padding: "6px 12px",
                backgroundColor: streamLoading ? "#94a3b8" : "#10b981",
                border: "none",
                color: "white",
                borderRadius: "4px",
                cursor: streamLoading ? "default" : "pointer",
              }}
            >
              {streamLoading ? "Refreshing…" : "Refresh"}
            </button>
            <label style={{ display: "flex", alignItems: "center", gap: "6px" }}>
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(event) => setAutoRefresh(event.target.checked)}
              />
              Auto refresh
            </label>
          </div>
          {streamError && (
            <p style={{ color: "#dc2626" }}>{streamError}</p>
          )}
          <div
            style={{
              maxHeight: "260px",
              overflow: "auto",
              backgroundColor: "#0f172a",
              color: "#f8fafc",
              padding: "12px",
              borderRadius: "6px",
              fontSize: "12px",
              lineHeight: 1.4,
            }}
          >
            {filteredMessages.length === 0 ? (
              <p style={{ margin: 0, color: "#94a3b8" }}>
                {streamLoading ? "Loading messages…" : "No messages available."}
              </p>
            ) : (
              filteredMessages.map((message, index) => (
                <article
                  key={`${message.topic}-${message.offset ?? index}-${message.timestamp ?? index}`}
                  style={{ marginBottom: "12px" }}
                >
                  <header style={{ marginBottom: "4px", color: "#38bdf8" }}>
                    <strong>{message.topic}</strong>
                    {message.partition !== undefined && (
                      <span> · Partition {message.partition}</span>
                    )}
                    {message.offset !== undefined && (
                      <span> · Offset {message.offset}</span>
                    )}
                    {message.timestamp && (
                      <span> · {formatTimestamp(message.timestamp)}</span>
                    )}
                  </header>
                  {message.correlationId && (
                    <div style={{ color: "#facc15" }}>
                      correlation_id: {message.correlationId}
                    </div>
                  )}
                  {message.key && <div>key: {message.key}</div>}
                  <pre
                    style={{
                      margin: "4px 0 0",
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                    }}
                  >
                    {typeof message.value === "string"
                      ? message.value
                      : JSON.stringify(message.value, null, 2)}
                  </pre>
                </article>
              ))
            )}
          </div>
        </section>

        <section
          style={{
            border: "1px solid #e2e8f0",
            borderRadius: "8px",
            padding: "16px",
            backgroundColor: "#fff",
          }}
        >
          <h3 style={{ marginTop: 0 }}>Chaos Toggles</h3>
          <p style={{ marginTop: 0, color: "#555" }}>
            Toggle fault injection scenarios for staging validation. These actions
            call the <code>/chaos</code> control plane.
          </p>
          <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
            <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <input
                type="checkbox"
                checked={safeModeChaos}
                onChange={(event) => toggleChaos("safe_mode", event.target.checked)}
                disabled={chaosLoading === "safe_mode"}
              />
              Simulate Safe Mode Entry
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <input
                type="checkbox"
                checked={latencyChaos}
                onChange={(event) => toggleChaos("latency", event.target.checked)}
                disabled={chaosLoading === "latency"}
              />
              Inject Sequencer Latency
            </label>
          </div>
          {chaosMessage && (
            <p style={{ color: "#16a34a", marginTop: "12px" }}>{chaosMessage}</p>
          )}
          {chaosError && (
            <p style={{ color: "#dc2626", marginTop: "12px" }}>{chaosError}</p>
          )}
        </section>

        <section
          style={{
            border: "1px solid #e2e8f0",
            borderRadius: "8px",
            padding: "16px",
            backgroundColor: "#fff",
          }}
        >
          <h3 style={{ marginTop: 0 }}>Audit Trail (Last 100)</h3>
          <div
            style={{
              display: "flex",
              gap: "8px",
              marginBottom: "12px",
              flexWrap: "wrap",
            }}
          >
            <label style={{ flex: "1 1 200px" }}>
              <span style={{ display: "block" }}>Search</span>
              <input
                type="text"
                value={auditQuery}
                onChange={(event) => setAuditQuery(event.target.value)}
                placeholder="Filter by actor, action, entity, timestamp, details"
                style={{
                  width: "100%",
                  padding: "8px",
                  marginTop: "4px",
                  borderRadius: "4px",
                  border: "1px solid #cbd5f5",
                }}
              />
            </label>
            <button
              type="button"
              onClick={() => {
                void fetchAuditEntries();
              }}
              disabled={auditLoading}
              style={{
                padding: "8px 16px",
                alignSelf: "flex-end",
                backgroundColor: auditLoading ? "#94a3b8" : "#2563eb",
                border: "none",
                color: "white",
                borderRadius: "4px",
                cursor: auditLoading ? "default" : "pointer",
              }}
            >
              {auditLoading ? "Refreshing…" : "Refresh"}
            </button>
          </div>
          {auditError && (
            <p style={{ color: "#dc2626" }}>{auditError}</p>
          )}
          <div
            style={{
              maxHeight: "280px",
              overflow: "auto",
              border: "1px solid #e2e8f0",
              borderRadius: "6px",
            }}
          >
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead style={{ backgroundColor: "#f1f5f9", position: "sticky", top: 0 }}>
                <tr>
                  <th
                    style={{
                      textAlign: "left",
                      padding: "8px",
                      borderBottom: "1px solid #e2e8f0",
                    }}
                  >
                    Timestamp
                  </th>
                  <th
                    style={{
                      textAlign: "left",
                      padding: "8px",
                      borderBottom: "1px solid #e2e8f0",
                    }}
                  >
                    Actor
                  </th>
                  <th
                    style={{
                      textAlign: "left",
                      padding: "8px",
                      borderBottom: "1px solid #e2e8f0",
                    }}
                  >
                    Action
                  </th>
                  <th
                    style={{
                      textAlign: "left",
                      padding: "8px",
                      borderBottom: "1px solid #e2e8f0",
                    }}
                  >
                    Entity
                  </th>
                  <th
                    style={{
                      textAlign: "left",
                      padding: "8px",
                      borderBottom: "1px solid #e2e8f0",
                    }}
                  >
                    Details
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredAuditEntries.length === 0 ? (
                  <tr>
                    <td colSpan={5} style={{ padding: "12px", textAlign: "center" }}>
                      {auditLoading ? "Loading audit events…" : "No audit events found."}
                    </td>
                  </tr>
                ) : (
                  filteredAuditEntries.map((entry) => (
                    <tr key={entry.id ?? `${entry.actor}-${entry.ts}`}>
                      <td
                        style={{
                          padding: "8px",
                          borderBottom: "1px solid #f1f5f9",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {formatTimestamp(entry.ts)}
                      </td>
                      <td
                        style={{
                          padding: "8px",
                          borderBottom: "1px solid #f1f5f9",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {entry.actor || "—"}
                      </td>
                      <td
                        style={{
                          padding: "8px",
                          borderBottom: "1px solid #f1f5f9",
                        }}
                      >
                        {entry.action || "—"}
                      </td>
                      <td
                        style={{
                          padding: "8px",
                          borderBottom: "1px solid #f1f5f9",
                        }}
                      >
                        {entry.entity || "—"}
                      </td>
                      <td
                        style={{
                          padding: "8px",
                          borderBottom: "1px solid #f1f5f9",
                          maxWidth: "260px",
                        }}
                      >
                        <pre
                          style={{
                            margin: 0,
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                            fontSize: "12px",
                          }}
                        >
                          {entry.details || "—"}
                        </pre>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </section>
  );
};

export default DevSandbox;
