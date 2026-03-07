import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getPresetProfiles } from "../lib/api";

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

interface ProfilesPanelProps {
  selectedSymbol: string;
}

export default function ProfilesPanel({ selectedSymbol }: ProfilesPanelProps) {
  const [lookbackDays, setLookbackDays] = useState("5");
  const [preset, setPreset] = useState("day");
  const [timezone, setTimezone] = useState("America/New_York");
  const [metric, setMetric] = useState("volume");
  const [tickAggregation, setTickAggregation] = useState("1");
  const [valueAreaEnabled, setValueAreaEnabled] = useState(true);

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
    enabled: Boolean(selectedSymbol),
  });

  return (
    <section className="panel-grid">
      <article className="panel control-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Profile controls</p>
            <h2>{selectedSymbol || "No symbol selected"}</h2>
          </div>
          <span className="pill">{profilesQuery.data?.profiles.length ?? 0} segments</span>
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
            <span className="field-label">Preset</span>
            <select className="field-input" value={preset} onChange={(event) => setPreset(event.target.value)}>
              <option value="day">Day</option>
              <option value="week">Week</option>
              <option value="rth">RTH</option>
              <option value="eth">ETH</option>
            </select>
          </label>

          <label className="field">
            <span className="field-label">Metric</span>
            <select className="field-input" value={metric} onChange={(event) => setMetric(event.target.value)}>
              <option value="volume">Volume</option>
              <option value="delta">Delta</option>
            </select>
          </label>

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
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Profiles</p>
            <h2>Preset persisted profiles</h2>
          </div>
          <span className="pill">{profilesQuery.data?.profiles.length ?? 0} segments</span>
        </div>
        {!selectedSymbol ? <p>Select a symbol from the sidebar to begin.</p> : null}
        {profilesQuery.isLoading ? <p>Loading profiles…</p> : null}
        {profilesQuery.isError ? <p>Unable to load preset profiles from the Rust API.</p> : null}
        <div className="stack">
          {profilesQuery.data?.profiles.map((profile) => (
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
          ))}
        </div>
      </article>
    </section>
  );
}
