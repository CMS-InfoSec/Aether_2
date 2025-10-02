import React, {
  FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { formatLondonTime } from "./timezone";
import { useAuthClaims } from "./useAuthClaims";

type TrainingStartResponse = {
  status?: string;
  job_id?: string | number;
  detail?: string;
  message?: string;
  [key: string]: unknown;
};

type TrainingJobStatus = {
  job_id?: string | number;
  id?: string | number;
  symbol?: string;
  symbols?: string[];
  state?: string;
  status?: string;
  stage?: string;
  progress?: number;
  percent_complete?: number;
  message?: string;
  started_at?: string;
  updated_at?: string;
  completed_at?: string;
  [key: string]: unknown;
};

type TrainingStatusResponse = {
  jobs?: TrainingJobStatus[];
  runs?: TrainingJobStatus[];
  statuses?: TrainingJobStatus[];
  data?: TrainingJobStatus[];
  updated_at?: string;
  message?: string;
  [key: string]: unknown;
};

type ActiveModelEntry = {
  symbol: string;
  strategy: string;
  model_id: string;
  stage: string;
  actor: string;
  ts: string;
};

type ActiveModelsResponse = {
  active?: ActiveModelEntry[];
};

const TRAINING_STATUS_POLL_INTERVAL_MS = 5000;
const ACTIVE_MODELS_POLL_INTERVAL_MS = 60000;

const GRANULARITY_OPTIONS = ["1m", "5m", "15m", "1h", "4h", "1d"];
const MODEL_TYPE_OPTIONS = ["policy", "alpha", "execution"];

const normalizeTimestamp = (value?: string | number) => {
  if (!value) {
    return "—";
  }

  const formatted = formatLondonTime(value ?? null);
  if (!formatted) {
    return typeof value === "string" ? value : String(value);
  }

  return formatted;
};

const normalizeProgress = (value: unknown): string => {
  if (typeof value === "number" && Number.isFinite(value)) {
    if (value >= 0 && value <= 1) {
      return `${Math.round(value * 100)}%`;
    }
    return `${Math.round(value)}%`;
  }
  if (typeof value === "string" && value.trim()) {
    const numeric = Number(value);
    if (!Number.isNaN(numeric)) {
      if (numeric >= 0 && numeric <= 1) {
        return `${Math.round(numeric * 100)}%`;
      }
      return `${Math.round(numeric)}%`;
    }
    return value;
  }
  return "—";
};

const extractTrainingJobs = (
  payload: TrainingStatusResponse | null
): TrainingJobStatus[] => {
  if (!payload) {
    return [];
  }

  const candidateKeys: (keyof TrainingStatusResponse)[] = [
    "jobs",
    "runs",
    "statuses",
    "data",
  ];

  for (const key of candidateKeys) {
    const value = payload[key];
    if (Array.isArray(value)) {
      return value.filter(
        (item): item is TrainingJobStatus =>
          Boolean(item) && typeof item === "object"
      );
    }
  }

  return [];
};

const TrainingPanel: React.FC = () => {
  const { claims } = useAuthClaims();
  const role = useMemo(
    () => (claims?.role ? String(claims.role).toLowerCase() : ""),
    [claims]
  );
  const isAdmin = role === "admin";

  const [selectedSymbols, setSelectedSymbols] = useState<string[]>([]);
  const [manualSymbol, setManualSymbol] = useState("");
  const [startDate, setStartDate] = useState<string>("");
  const [endDate, setEndDate] = useState<string>("");
  const [granularity, setGranularity] = useState<string>("1h");
  const [modelType, setModelType] = useState<string>(MODEL_TYPE_OPTIONS[0]);
  const [curriculumEnabled, setCurriculumEnabled] = useState<boolean>(false);
  const [featureVersion, setFeatureVersion] = useState<string>("");
  const [labelHorizon, setLabelHorizon] = useState<string>("");

  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitSuccess, setSubmitSuccess] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

  const [statusPayload, setStatusPayload] = useState<TrainingStatusResponse | null>(
    null
  );
  const [statusLoading, setStatusLoading] = useState<boolean>(true);
  const [statusError, setStatusError] = useState<string | null>(null);

  const [activeModels, setActiveModels] = useState<ActiveModelEntry[]>([]);
  const [modelsLoading, setModelsLoading] = useState<boolean>(true);
  const [modelsError, setModelsError] = useState<string | null>(null);

  const availableSymbols = useMemo(() => {
    const set = new Set<string>();
    for (const entry of activeModels) {
      if (entry?.symbol) {
        set.add(String(entry.symbol).toUpperCase());
      }
    }
    for (const symbol of selectedSymbols) {
      if (symbol) {
        set.add(symbol.toUpperCase());
      }
    }
    return Array.from(set).sort();
  }, [activeModels, selectedSymbols]);

  const latestModelsBySymbol = useMemo(() => {
    const map = new Map<string, ActiveModelEntry>();
    for (const entry of activeModels) {
      const symbol = entry?.symbol ? String(entry.symbol).toUpperCase() : "";
      if (!symbol) {
        continue;
      }
      const existing = map.get(symbol);
      if (!existing) {
        map.set(symbol, entry);
        continue;
      }
      const currentTs = Date.parse(entry.ts);
      const existingTs = Date.parse(existing.ts);
      if (Number.isNaN(existingTs) && !Number.isNaN(currentTs)) {
        map.set(symbol, entry);
        continue;
      }
      if (!Number.isNaN(currentTs) && !Number.isNaN(existingTs)) {
        if (currentTs > existingTs) {
          map.set(symbol, entry);
        }
        continue;
      }
      if (entry.ts > existing.ts) {
        map.set(symbol, entry);
      }
    }
    return Array.from(map.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([, entry]) => entry);
  }, [activeModels]);

  const trainingJobs = useMemo(
    () => extractTrainingJobs(statusPayload),
    [statusPayload]
  );

  const handleSymbolsChange = (
    event: React.ChangeEvent<HTMLSelectElement>
  ) => {
    const values = Array.from(event.target.selectedOptions).map((option) =>
      option.value.trim().toUpperCase()
    );
    setSelectedSymbols(values);
  };

  const handleAddManualSymbol = useCallback(() => {
    const normalized = manualSymbol.trim().toUpperCase();
    if (!normalized) {
      return;
    }
    setSelectedSymbols((prev) => {
      if (prev.includes(normalized)) {
        return prev;
      }
      return [...prev, normalized];
    });
    setManualSymbol("");
  }, [manualSymbol]);

  const submitTrainingJob = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmitError(null);
    setSubmitSuccess(null);

    if (!selectedSymbols.length) {
      setSubmitError("Select at least one symbol to start training.");
      return;
    }

    if (!startDate || !endDate) {
      setSubmitError("Provide both start and end dates for the training window.");
      return;
    }

    if (new Date(startDate) > new Date(endDate)) {
      setSubmitError("The start date must be before the end date.");
      return;
    }

    const payload: Record<string, unknown> = {
      symbols: selectedSymbols,
      start_date: startDate,
      end_date: endDate,
      granularity,
      model_type: modelType,
      curriculum: curriculumEnabled,
    };

    const featureVersionValue = featureVersion.trim();
    if (featureVersionValue) {
      const numeric = Number(featureVersionValue);
      payload.feature_version = Number.isNaN(numeric)
        ? featureVersionValue
        : numeric;
    }

    const labelHorizonValue = labelHorizon.trim();
    if (labelHorizonValue) {
      const numeric = Number(labelHorizonValue);
      payload.label_horizon = Number.isNaN(numeric)
        ? labelHorizonValue
        : numeric;
    }

    setIsSubmitting(true);

    try {
      const response = await fetch("/ml/train/start", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(
          `Training request failed (${response.status}): ${text || "Unknown error"}`
        );
      }

      const data = (await response.json()) as TrainingStartResponse;
      const jobId = data.job_id ?? data.status ?? "submitted";
      setSubmitSuccess(`Training job submitted (${jobId}).`);
      setSubmitError(null);
    } catch (error) {
      console.error("Failed to start training", error);
      setSubmitError(
        error instanceof Error
          ? error.message
          : "Unable to start training."
      );
      setSubmitSuccess(null);
    } finally {
      setIsSubmitting(false);
    }
  };

  useEffect(() => {
    let timeoutId: number | undefined;
    let abortController: AbortController | null = null;
    let cancelled = false;
    let initial = true;

    const fetchStatus = async () => {
      if (cancelled) {
        return;
      }

      if (abortController) {
        abortController.abort();
      }
      abortController = new AbortController();

      try {
        if (initial) {
          setStatusLoading(true);
        }
        const response = await fetch("/ml/train/status", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal: abortController.signal,
        });

        if (!response.ok) {
          throw new Error(`Status request failed (${response.status}).`);
        }

        const payload = (await response.json()) as TrainingStatusResponse;
        setStatusPayload(payload);
        setStatusError(null);
      } catch (error) {
        if (abortController.signal.aborted) {
          return;
        }
        console.error("Failed to load training status", error);
        setStatusError("Unable to load training status.");
      } finally {
        if (!abortController.signal.aborted) {
          setStatusLoading(false);
          initial = false;
        }
      }

      if (!cancelled) {
        timeoutId = window.setTimeout(fetchStatus, TRAINING_STATUS_POLL_INTERVAL_MS);
      }
    };

    fetchStatus();

    return () => {
      cancelled = true;
      if (typeof timeoutId === "number") {
        window.clearTimeout(timeoutId);
      }
      if (abortController) {
        abortController.abort();
      }
    };
  }, []);

  useEffect(() => {
    let timeoutId: number | undefined;
    let abortController: AbortController | null = null;
    let cancelled = false;
    let initial = true;

    const fetchActiveModels = async () => {
      if (cancelled) {
        return;
      }

      if (abortController) {
        abortController.abort();
      }
      abortController = new AbortController();

      try {
        if (initial) {
          setModelsLoading(true);
        }
        const response = await fetch("/models/active", {
          method: "GET",
          headers: { Accept: "application/json" },
          signal: abortController.signal,
        });

        if (!response.ok) {
          throw new Error(`Active models request failed (${response.status}).`);
        }

        const payload = (await response.json()) as ActiveModelsResponse;
        const entries = Array.isArray(payload.active) ? payload.active : [];
        setActiveModels(entries);
        setModelsError(null);
      } catch (error) {
        if (abortController.signal.aborted) {
          return;
        }
        console.error("Failed to load active models", error);
        setModelsError("Unable to load active models.");
      } finally {
        if (!abortController.signal.aborted) {
          setModelsLoading(false);
          initial = false;
        }
      }

      if (!cancelled) {
        timeoutId = window.setTimeout(
          fetchActiveModels,
          ACTIVE_MODELS_POLL_INTERVAL_MS
        );
      }
    };

    fetchActiveModels();

    return () => {
      cancelled = true;
      if (typeof timeoutId === "number") {
        window.clearTimeout(timeoutId);
      }
      if (abortController) {
        abortController.abort();
      }
    };
  }, []);

  if (!isAdmin) {
    return (
      <section className="training-panel">
        <h2>Model Training</h2>
        <p>Access restricted to administrators.</p>
      </section>
    );
  }

  return (
    <section className="training-panel">
      <h2>Model Training</h2>

      <form className="training-form" onSubmit={submitTrainingJob}>
        <fieldset disabled={isSubmitting}>
          <legend>Start a new training run</legend>

          <div className="form-row">
            <label htmlFor="training-symbols">Symbols</label>
            <select
              id="training-symbols"
              multiple
              value={selectedSymbols}
              onChange={handleSymbolsChange}
            >
              {availableSymbols.map((symbol) => (
                <option key={symbol} value={symbol}>
                  {symbol}
                </option>
              ))}
            </select>
            <div className="manual-symbol">
              <label htmlFor="manual-symbol-input" className="sr-only">
                Add symbol
              </label>
              <input
                id="manual-symbol-input"
                type="text"
                value={manualSymbol}
                onChange={(event) => setManualSymbol(event.target.value)}
                placeholder="Add symbol"
              />
              <button
                type="button"
                onClick={handleAddManualSymbol}
                disabled={!manualSymbol.trim()}
              >
                Add
              </button>
            </div>
          </div>

          <div className="form-row">
            <label htmlFor="training-start-date">Start date</label>
            <input
              id="training-start-date"
              type="date"
              value={startDate}
              onChange={(event) => setStartDate(event.target.value)}
              required
            />
          </div>

          <div className="form-row">
            <label htmlFor="training-end-date">End date</label>
            <input
              id="training-end-date"
              type="date"
              value={endDate}
              onChange={(event) => setEndDate(event.target.value)}
              required
            />
          </div>

          <div className="form-row">
            <label htmlFor="training-granularity">Granularity</label>
            <select
              id="training-granularity"
              value={granularity}
              onChange={(event) => setGranularity(event.target.value)}
            >
              {GRANULARITY_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </div>

          <div className="form-row">
            <label htmlFor="training-model-type">Model type</label>
            <select
              id="training-model-type"
              value={modelType}
              onChange={(event) => setModelType(event.target.value)}
            >
              {MODEL_TYPE_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </div>

          <div className="form-row checkbox-row">
            <label htmlFor="training-curriculum">
              <input
                id="training-curriculum"
                type="checkbox"
                checked={curriculumEnabled}
                onChange={(event) => setCurriculumEnabled(event.target.checked)}
              />
              Enable curriculum learning
            </label>
          </div>

          <div className="form-row">
            <label htmlFor="training-feature-version">Feature version</label>
            <input
              id="training-feature-version"
              type="text"
              value={featureVersion}
              onChange={(event) => setFeatureVersion(event.target.value)}
              placeholder="e.g. 42"
            />
          </div>

          <div className="form-row">
            <label htmlFor="training-label-horizon">Label horizon</label>
            <input
              id="training-label-horizon"
              type="number"
              value={labelHorizon}
              onChange={(event) => setLabelHorizon(event.target.value)}
              min={1}
              placeholder="e.g. 60"
            />
          </div>

          {submitError && <p className="form-error">{submitError}</p>}
          {submitSuccess && <p className="form-success">{submitSuccess}</p>}

          <div className="form-actions">
            <button type="submit" disabled={isSubmitting}>
              {isSubmitting ? "Submitting…" : "Start training"}
            </button>
          </div>
        </fieldset>
      </form>

      <section className="training-status">
        <h3>Training status</h3>
        {statusLoading && <p>Loading status…</p>}
        {statusError && <p className="form-error">{statusError}</p>}
        {!statusLoading && !statusError && trainingJobs.length === 0 && (
          <pre>{JSON.stringify(statusPayload ?? {}, null, 2)}</pre>
        )}
        {!statusLoading && !statusError && trainingJobs.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>Job</th>
                <th>Symbols</th>
                <th>Status</th>
                <th>Progress</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {trainingJobs.map((job, index) => {
                const jobId =
                  job.job_id ?? job.id ?? job.symbol ?? `job-${index + 1}`;
                const symbols = Array.isArray(job.symbols)
                  ? job.symbols.join(", ")
                  : job.symbol || "—";
                const status = job.state || job.status || job.stage || "—";
                const progress = normalizeProgress(
                  job.progress ?? job.percent_complete
                );
                const updatedAt = normalizeTimestamp(
                  job.updated_at || job.completed_at || job.started_at
                );
                const message = job.message ? ` (${job.message})` : "";
                return (
                  <tr key={String(jobId)}>
                    <td>{String(jobId)}</td>
                    <td>{symbols}</td>
                    <td>{`${status}${message}`}</td>
                    <td>{progress}</td>
                    <td>{updatedAt}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </section>

      <section className="active-models">
        <h3>Latest active models</h3>
        {modelsLoading && <p>Loading models…</p>}
        {modelsError && <p className="form-error">{modelsError}</p>}
        {!modelsLoading && !modelsError && latestModelsBySymbol.length === 0 && (
          <p>No active models found.</p>
        )}
        {!modelsLoading && !modelsError && latestModelsBySymbol.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Strategy</th>
                <th>Model ID</th>
                <th>Stage</th>
                <th>Actor</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {latestModelsBySymbol.map((entry) => (
                <tr key={`${entry.symbol}-${entry.strategy}`}>
                  <td>{entry.symbol}</td>
                  <td>{entry.strategy}</td>
                  <td>{entry.model_id}</td>
                  <td>{entry.stage}</td>
                  <td>{entry.actor}</td>
                  <td>{normalizeTimestamp(entry.ts)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </section>
  );
};

export default TrainingPanel;
