import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import ChartCard from "../components/ChartCard";
import { getBars, getLargeOrders, getPresetProfiles } from "../lib/api";

function buildBarsParams(lookbackHours: number, barType: string, timeframe: string, barSize: string) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * lookbackHours);
  const params = new URLSearchParams({
    timeframe,
    start: start.toISOString(),
    end: end.toISOString(),
    bar_type: barType,
  });

  if (barType !== "time") {
    params.set("bar_size", barSize);
  }

  return params;
}

const BAR_SIZE_OPTIONS = {
  tick: ["1500", "3000", "5000"],
  volume: ["500", "1000", "5000"],
  range: ["20", "40", "80"],
} as const;
const BASE_LARGE_ORDER_THRESHOLD = 25;
const CHART_PROFILE_MAX_SEGMENTS = 12;

interface ChartWorkspaceProps {
  selectedSymbol: string;
  onSelectedSymbolChange: (nextSymbol: string) => void;
  symbolOptions: string[];
}

function buildLargeOrdersParams(lookbackHours: number, fixedThreshold: string) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * lookbackHours);
  return new URLSearchParams({
    start: start.toISOString(),
    end: end.toISOString(),
    method: "fixed",
    fixed_threshold: fixedThreshold,
  });
}

function buildPresetProfileParams(
  lookbackHours: number,
  preset: string,
  timezone: string,
  metric: string,
  tickAggregation: string,
  valueAreaEnabled: boolean,
) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * lookbackHours);
  return new URLSearchParams({
    start: start.toISOString(),
    end: end.toISOString(),
    preset,
    timezone,
    metric,
    tick_aggregation: tickAggregation,
    value_area_enabled: String(valueAreaEnabled),
    value_area_percent: "70",
    max_segments: String(CHART_PROFILE_MAX_SEGMENTS),
  });
}

function normalizeLargeOrderThreshold(value: string) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < BASE_LARGE_ORDER_THRESHOLD) {
    return BASE_LARGE_ORDER_THRESHOLD;
  }

  return numeric;
}

export default function ChartWorkspace({
  selectedSymbol,
  onSelectedSymbolChange,
  symbolOptions,
}: ChartWorkspaceProps) {
  const [lookbackHours, setLookbackHours] = useState("24");
  const [barType, setBarType] = useState("time");
  const [timeframe, setTimeframe] = useState("1m");
  const [barSize, setBarSize] = useState("1500");
  const [largeOrderThreshold, setLargeOrderThreshold] = useState("25");
  const [showVolume, setShowVolume] = useState(true);
  const [showLargeOrders, setShowLargeOrders] = useState(false);
  const [showProfiles, setShowProfiles] = useState(false);
  const [profilePreset, setProfilePreset] = useState("day");
  const [profileTimezone, setProfileTimezone] = useState("America/New_York");
  const [profileMetric, setProfileMetric] = useState("volume");
  const [profileAggregation, setProfileAggregation] = useState("1");
  const [profileValueAreaEnabled, setProfileValueAreaEnabled] = useState(true);
  const normalizedLargeOrderThreshold = normalizeLargeOrderThreshold(largeOrderThreshold);

  const availableBarSizes = useMemo(
    () => (barType === "time" ? [] : [...BAR_SIZE_OPTIONS[barType as keyof typeof BAR_SIZE_OPTIONS]]),
    [barType],
  );

  useEffect(() => {
    if (barType === "time") {
      return;
    }

    if (!availableBarSizes.some((size) => size === barSize)) {
      setBarSize(availableBarSizes[0]);
    }
  }, [availableBarSizes, barSize, barType]);

  const barsQuery = useQuery({
    queryKey: ["bars", selectedSymbol, lookbackHours, barType, timeframe, barSize],
    queryFn: () =>
      getBars(
        selectedSymbol,
        buildBarsParams(Number(lookbackHours), barType, timeframe, barSize),
      ),
    enabled: Boolean(selectedSymbol),
    placeholderData: (previousData) => previousData,
  });

  const largeOrdersQuery = useQuery({
    queryKey: ["large-orders", selectedSymbol, lookbackHours, BASE_LARGE_ORDER_THRESHOLD],
    queryFn: () =>
      getLargeOrders(
        selectedSymbol,
        buildLargeOrdersParams(Number(lookbackHours), String(BASE_LARGE_ORDER_THRESHOLD)),
      ),
    enabled: Boolean(selectedSymbol) && showLargeOrders,
    placeholderData: (previousData) => previousData,
  });

  const filteredLargeOrders = useMemo(() => {
    const rows = largeOrdersQuery.data?.large_orders ?? [];
    return rows.filter((row) => row.trade_size >= normalizedLargeOrderThreshold);
  }, [largeOrdersQuery.data?.large_orders, normalizedLargeOrderThreshold]);

  const presetProfilesQuery = useQuery({
    queryKey: [
      "chart-preset-profiles",
      selectedSymbol,
      lookbackHours,
      profilePreset,
      profileTimezone,
      profileMetric,
      profileAggregation,
      profileValueAreaEnabled,
    ],
    queryFn: () =>
      getPresetProfiles(
        selectedSymbol,
        buildPresetProfileParams(
          Number(lookbackHours),
          profilePreset,
          profileTimezone,
          profileMetric,
          profileAggregation,
          profileValueAreaEnabled,
        ),
      ),
    enabled: Boolean(selectedSymbol) && showProfiles,
    placeholderData: (previousData) => previousData,
  });

  const chartProfiles = presetProfilesQuery.isError ? [] : presetProfilesQuery.data?.profiles ?? [];

  const seriesLabel = barType === "time" ? timeframe : `${barType} ${barSize}`;
  const loadStateLabel = barsQuery.isError
    ? "Bars unavailable"
    : barsQuery.isFetching
      ? "Refreshing"
      : barsQuery.data?.bars.length
        ? "Live"
        : "Empty";

  return (
    <section className="chart-workspace">
      <header className="chart-toolbar">
        <div className="chart-toolbar-main">
          <div className="chart-title">
            <p className="eyebrow">Chart</p>
            <h2>{selectedSymbol || "No contract selected"}</h2>
          </div>

          <label className="workspace-field workspace-field-symbol">
            <span className="workspace-field-label">Symbol</span>
            <select
              aria-label="Select active symbol"
              className="field-input"
              value={selectedSymbol}
              onChange={(event) => onSelectedSymbolChange(event.target.value)}
            >
              {!symbolOptions.length ? <option value="">No symbols yet</option> : null}
              {symbolOptions.map((symbol) => (
                <option key={symbol} value={symbol}>
                  {symbol}
                </option>
              ))}
            </select>
          </label>

          <label className="workspace-field">
            <span className="workspace-field-label">Lookback</span>
            <select
              className="field-input"
              value={lookbackHours}
              onChange={(event) => setLookbackHours(event.target.value)}
            >
              <option value="6">6 hours</option>
              <option value="24">24 hours</option>
              <option value="72">3 days</option>
              <option value="168">7 days</option>
            </select>
          </label>

          <label className="workspace-field">
            <span className="workspace-field-label">Bar type</span>
            <select className="field-input" value={barType} onChange={(event) => setBarType(event.target.value)}>
              <option value="time">Time</option>
              <option value="tick">Tick</option>
              <option value="volume">Volume</option>
              <option value="range">Range</option>
            </select>
          </label>

          {barType === "time" ? (
            <label className="workspace-field">
              <span className="workspace-field-label">Timeframe</span>
              <select className="field-input" value={timeframe} onChange={(event) => setTimeframe(event.target.value)}>
                <option value="1m">1m</option>
                <option value="3m">3m</option>
                <option value="5m">5m</option>
                <option value="15m">15m</option>
                <option value="30m">30m</option>
                <option value="60m">60m</option>
              </select>
            </label>
          ) : (
            <label className="workspace-field">
              <span className="workspace-field-label">Bar size</span>
              <select className="field-input" value={barSize} onChange={(event) => setBarSize(event.target.value)}>
                {availableBarSizes.map((size) => (
                  <option key={size} value={size}>
                    {barType === "range" ? `${size} ticks` : size}
                  </option>
                ))}
              </select>
            </label>
          )}

          <label className="workspace-field workspace-field-threshold">
            <span className="workspace-field-label">Large threshold</span>
            <input
              className="field-input"
              disabled={!showLargeOrders}
              inputMode="numeric"
              min={BASE_LARGE_ORDER_THRESHOLD}
              value={largeOrderThreshold}
              onChange={(event) => setLargeOrderThreshold(event.target.value)}
            />
          </label>

          <div className="workspace-toggle-row">
            <label className="workspace-toggle">
              <input
                checked={showVolume}
                type="checkbox"
                onChange={(event) => setShowVolume(event.target.checked)}
              />
              <span>Volume</span>
            </label>
            <label className="workspace-toggle">
              <input
                checked={showLargeOrders}
                type="checkbox"
                onChange={(event) => setShowLargeOrders(event.target.checked)}
              />
              <span>Large orders</span>
            </label>
            <label className="workspace-toggle">
              <input
                checked={showProfiles}
                type="checkbox"
                onChange={(event) => setShowProfiles(event.target.checked)}
              />
              <span>Profiles</span>
            </label>
          </div>

          {showProfiles ? (
            <>
              <label className="workspace-field">
                <span className="workspace-field-label">Profile preset</span>
                <select className="field-input" value={profilePreset} onChange={(event) => setProfilePreset(event.target.value)}>
                  <option value="day">Day</option>
                  <option value="week">Week</option>
                  <option value="rth">RTH</option>
                  <option value="eth">ETH</option>
                </select>
              </label>

              <label className="workspace-field">
                <span className="workspace-field-label">Profile timezone</span>
                <select
                  className="field-input"
                  value={profileTimezone}
                  onChange={(event) => setProfileTimezone(event.target.value)}
                >
                  <option value="America/New_York">America/New_York</option>
                  <option value="America/Chicago">America/Chicago</option>
                  <option value="UTC">UTC</option>
                </select>
              </label>

              <label className="workspace-field">
                <span className="workspace-field-label">Profile metric</span>
                <select className="field-input" value={profileMetric} onChange={(event) => setProfileMetric(event.target.value)}>
                  <option value="volume">Volume</option>
                  <option value="delta">Delta</option>
                </select>
              </label>

              <label className="workspace-field">
                <span className="workspace-field-label">Aggregation</span>
                <select
                  className="field-input"
                  value={profileAggregation}
                  onChange={(event) => setProfileAggregation(event.target.value)}
                >
                  <option value="1">1x</option>
                  <option value="2">2x</option>
                  <option value="4">4x</option>
                  <option value="8">8x</option>
                </select>
              </label>

              <label className="workspace-toggle">
                <input
                  checked={profileValueAreaEnabled}
                  type="checkbox"
                  onChange={(event) => setProfileValueAreaEnabled(event.target.checked)}
                />
                <span>Value area</span>
              </label>
            </>
          ) : null}
        </div>

        <div className="chart-toolbar-meta">
          <span className="workspace-pill">{loadStateLabel}</span>
          <span className="workspace-pill">{seriesLabel}</span>
          <span className="workspace-pill">{barsQuery.data?.bars.length ?? 0} bars</span>
          {showProfiles ? <span className="workspace-pill">{chartProfiles.length} profiles</span> : null}
          {showLargeOrders ? (
            <span className="workspace-pill">
              {filteredLargeOrders.length} markers
            </span>
          ) : null}
        </div>
      </header>

      <div className="chart-stage">
        <div className="chart-surface">
          {!selectedSymbol ? (
            <div className="chart-empty-state">
              <p className="eyebrow">No symbol</p>
              <h3>Load a contract to begin.</h3>
              <p className="microcopy">Once symbols are available, the chart toolbar becomes the control center.</p>
            </div>
          ) : null}

          {selectedSymbol && barsQuery.isError ? (
            <div className="chart-empty-state">
              <p className="eyebrow">Bars error</p>
              <h3>Unable to load candles from the Rust API.</h3>
              <p className="microcopy">{barsQuery.error.message}</p>
            </div>
          ) : null}

          {selectedSymbol && !barsQuery.isError && !barsQuery.data?.bars.length ? (
            <div className="chart-empty-state">
              <p className="eyebrow">No bars</p>
              <h3>No candles in the selected range.</h3>
              <p className="microcopy">Try a wider lookback window or a different bar configuration.</p>
            </div>
          ) : null}

          {barsQuery.data?.bars.length ? (
            <>
              <ChartCard
                bars={barsQuery.data.bars}
                largeOrders={showLargeOrders ? filteredLargeOrders : []}
                profiles={showProfiles ? chartProfiles : []}
                showProfiles={showProfiles}
                showLargeOrders={showLargeOrders}
                showProfileValueArea={profileValueAreaEnabled}
                showVolume={showVolume}
              />
              {barsQuery.isFetching ? <div className="chart-status-banner">Refreshing chart data…</div> : null}
            </>
          ) : null}
        </div>
      </div>

      <footer className="chart-footer">
        <div>
          <p className="eyebrow">Data source</p>
          <p className="microcopy">
            Candles read from persisted market artifacts. Large-order markers are filtered client-side from the persisted 25+ stream until lower thresholds are rebuilt. Preset profiles use the persisted profile segments API for the same chart window.
          </p>
        </div>
        <div className="chart-footer-metrics">
          <div className="chart-footer-card">
            <span className="metric-label">Window</span>
            <strong>{lookbackHours}h</strong>
          </div>
          <div className="chart-footer-card">
            <span className="metric-label">Series</span>
            <strong>{seriesLabel}</strong>
          </div>
        </div>
      </footer>
    </section>
  );
}
