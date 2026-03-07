import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getAreaProfile, getPresetProfiles } from "../lib/api";

function buildProfileParams(
  lookbackDays: string,
  preset: string,
  timezone: string,
  metric: string,
  tickAggregation: string,
  valueAreaEnabled: boolean,
) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * Number(lookbackDays));
  return new URLSearchParams({
    start: start.toISOString(),
    end: end.toISOString(),
    preset,
    timezone,
    metric,
    tick_aggregation: tickAggregation,
    value_area_enabled: String(valueAreaEnabled),
    value_area_percent: "70",
    max_segments: "5",
  });
}

function buildAreaProfileParams(
  lookbackDays: string,
  timezone: string,
  metric: string,
  tickAggregation: string,
  valueAreaEnabled: boolean,
  priceMin: string,
  priceMax: string,
) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * Number(lookbackDays));
  return new URLSearchParams({
    start: start.toISOString(),
    end: end.toISOString(),
    price_min: priceMin || "0",
    price_max: priceMax || "1000000",
    timezone,
    metric,
    tick_aggregation: tickAggregation,
    value_area_enabled: String(valueAreaEnabled),
    value_area_percent: "70",
  });
}

interface ProfilesPanelProps {
  selectedSymbol: string;
}

export default function ProfilesPanel({ selectedSymbol }: ProfilesPanelProps) {
  const [mode, setMode] = useState<"preset" | "area">("preset");
  const [lookbackDays, setLookbackDays] = useState("5");
  const [preset, setPreset] = useState("day");
  const [timezone, setTimezone] = useState("America/New_York");
  const [metric, setMetric] = useState("volume");
  const [tickAggregation, setTickAggregation] = useState("1");
  const [valueAreaEnabled, setValueAreaEnabled] = useState(true);
  const [priceMin, setPriceMin] = useState("0");
  const [priceMax, setPriceMax] = useState("1000000");

  const profilesQuery = useQuery({
    queryKey: [
      "preset-profiles",
      selectedSymbol,
      lookbackDays,
      preset,
      timezone,
      metric,
      tickAggregation,
      valueAreaEnabled,
    ],
    queryFn: () =>
      getPresetProfiles(
        selectedSymbol,
        buildProfileParams(lookbackDays, preset, timezone, metric, tickAggregation, valueAreaEnabled),
      ),
    enabled: Boolean(selectedSymbol) && mode === "preset",
  });

  const areaProfileQuery = useQuery({
    queryKey: [
      "area-profile",
      selectedSymbol,
      lookbackDays,
      timezone,
      metric,
      tickAggregation,
      valueAreaEnabled,
      priceMin,
      priceMax,
    ],
    queryFn: () =>
      getAreaProfile(
        selectedSymbol,
        buildAreaProfileParams(
          lookbackDays,
          timezone,
          metric,
          tickAggregation,
          valueAreaEnabled,
          priceMin,
          priceMax,
        ),
      ),
    enabled: Boolean(selectedSymbol) && mode === "area",
  });

  return (
    <section className="panel-grid">
      <article className="panel control-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Profile controls</p>
            <h2>{selectedSymbol || "No symbol selected"}</h2>
          </div>
          <span className="pill">
            {mode === "preset" ? profilesQuery.data?.profiles.length ?? 0 : areaProfileQuery.data ? 1 : 0} profiles
          </span>
        </div>
        <div className="tab-row">
          <button
            className={mode === "preset" ? "subnav-button active" : "subnav-button"}
            onClick={() => setMode("preset")}
            type="button"
          >
            Preset
          </button>
          <button
            className={mode === "area" ? "subnav-button active" : "subnav-button"}
            onClick={() => setMode("area")}
            type="button"
          >
            Area
          </button>
        </div>
        <div className="form-grid">
          <label className="field">
            <span className="field-label">Lookback</span>
            <select className="field-input" value={lookbackDays} onChange={(event) => setLookbackDays(event.target.value)}>
              <option value="3">3 days</option>
              <option value="5">5 days</option>
              <option value="10">10 days</option>
              <option value="20">20 days</option>
            </select>
          </label>

          <label className="field">
            <span className="field-label">{mode === "preset" ? "Preset" : "Price min"}</span>
            {mode === "preset" ? (
              <select className="field-input" value={preset} onChange={(event) => setPreset(event.target.value)}>
                <option value="day">Day</option>
                <option value="week">Week</option>
                <option value="rth">RTH</option>
                <option value="eth">ETH</option>
              </select>
            ) : (
              <input className="field-input" value={priceMin} onChange={(event) => setPriceMin(event.target.value)} />
            )}
          </label>

          <label className="field">
            <span className="field-label">Metric</span>
            <select className="field-input" value={metric} onChange={(event) => setMetric(event.target.value)}>
              <option value="volume">Volume</option>
              <option value="delta">Delta</option>
            </select>
          </label>

          <label className="field">
            <span className="field-label">{mode === "preset" ? "Aggregation" : "Price max"}</span>
            {mode === "preset" ? (
            <select
              className="field-input"
              value={tickAggregation}
              onChange={(event) => setTickAggregation(event.target.value)}
            >
              <option value="1">1x</option>
              <option value="2">2x</option>
              <option value="4">4x</option>
              <option value="8">8x</option>
            </select>
            ) : (
              <input className="field-input" value={priceMax} onChange={(event) => setPriceMax(event.target.value)} />
            )}
          </label>

          {mode === "area" ? (
            <label className="field">
              <span className="field-label">Aggregation</span>
              <select
                className="field-input"
                value={tickAggregation}
                onChange={(event) => setTickAggregation(event.target.value)}
              >
                <option value="1">1x</option>
                <option value="2">2x</option>
                <option value="4">4x</option>
                <option value="8">8x</option>
              </select>
            </label>
          ) : null}

          <label className="field">
            <span className="field-label">Timezone</span>
            <select className="field-input" value={timezone} onChange={(event) => setTimezone(event.target.value)}>
              <option value="America/New_York">America/New_York</option>
              <option value="America/Chicago">America/Chicago</option>
              <option value="UTC">UTC</option>
            </select>
          </label>

          <label className="field checkbox-field">
            <input
              checked={valueAreaEnabled}
              type="checkbox"
              onChange={(event) => setValueAreaEnabled(event.target.checked)}
            />
            <span className="field-label">Value area</span>
          </label>
        </div>
        <p className="microcopy">
          {mode === "preset"
            ? "Preset profiles read persisted profile segments and base histogram levels."
            : "Area profiles are computed for the selected time and price window from stored market data."}
        </p>
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Profiles</p>
            <h2>{mode === "preset" ? "Preset persisted profiles" : "Area profile"}</h2>
          </div>
          <span className="pill">
            {mode === "preset" ? `${profilesQuery.data?.profiles.length ?? 0} segments` : `${priceMin} - ${priceMax}`}
          </span>
        </div>
        {!selectedSymbol ? <p>Select a symbol from the sidebar to begin.</p> : null}
        {mode === "preset" && profilesQuery.isLoading ? <p>Loading profiles…</p> : null}
        {mode === "preset" && profilesQuery.isError ? <p>Unable to load preset profiles from the Rust API.</p> : null}
        {mode === "area" && areaProfileQuery.isLoading ? <p>Loading area profile…</p> : null}
        {mode === "area" && areaProfileQuery.isError ? <p>Unable to load the area profile from the Rust API.</p> : null}
        <div className="stack">
          {mode === "preset"
            ? profilesQuery.data?.profiles.map((profile) => (
            <div className="profile-card" key={profile.id}>
              <div className="profile-header">
                <strong>{profile.label}</strong>
                <span>{profile.levels.length} levels</span>
              </div>
              <p className="muted">
                {new Date(profile.start).toLocaleString()} to {new Date(profile.end).toLocaleString()}
              </p>
              <div className="metric-grid">
                <div>
                  <span className="metric-label">Total</span>
                  <strong>{profile.total_value.toFixed(2)}</strong>
                </div>
                <div>
                  <span className="metric-label">POC</span>
                  <strong>{profile.value_area_poc?.toFixed(2) ?? "n/a"}</strong>
                </div>
                <div>
                  <span className="metric-label">Value area</span>
                  <strong>
                    {profile.value_area_low?.toFixed(2) ?? "n/a"} - {profile.value_area_high?.toFixed(2) ?? "n/a"}
                  </strong>
                </div>
                <div>
                  <span className="metric-label">Max level</span>
                  <strong>{profile.max_value.toFixed(2)}</strong>
                </div>
              </div>
              <p className="microcopy">
                {profile.levels.slice(0, 6).map((level) => `${level.price_level.toFixed(2)}:${level.value.toFixed(0)}`).join(" • ")}
              </p>
            </div>
              ))
            : null}
          {mode === "area" && areaProfileQuery.data ? (
            <div className="profile-card">
              <div className="profile-header">
                <strong>{areaProfileQuery.data.profile.label}</strong>
                <span>{areaProfileQuery.data.profile.levels.length} levels</span>
              </div>
              <p className="muted">
                {new Date(areaProfileQuery.data.profile.start).toLocaleString()} to{" "}
                {new Date(areaProfileQuery.data.profile.end).toLocaleString()}
              </p>
              <div className="metric-grid">
                <div>
                  <span className="metric-label">Total</span>
                  <strong>{areaProfileQuery.data.profile.total_value.toFixed(2)}</strong>
                </div>
                <div>
                  <span className="metric-label">POC</span>
                  <strong>{areaProfileQuery.data.profile.value_area_poc?.toFixed(2) ?? "n/a"}</strong>
                </div>
                <div>
                  <span className="metric-label">Value area</span>
                  <strong>
                    {areaProfileQuery.data.profile.value_area_low?.toFixed(2) ?? "n/a"} -{" "}
                    {areaProfileQuery.data.profile.value_area_high?.toFixed(2) ?? "n/a"}
                  </strong>
                </div>
                <div>
                  <span className="metric-label">Max level</span>
                  <strong>{areaProfileQuery.data.profile.max_value.toFixed(2)}</strong>
                </div>
              </div>
              <p className="microcopy">
                {areaProfileQuery.data.profile.levels
                  .slice(0, 10)
                  .map((level) => `${level.price_level.toFixed(2)}:${level.value.toFixed(0)}`)
                  .join(" • ")}
              </p>
            </div>
          ) : null}
        </div>
      </article>
    </section>
  );
}
