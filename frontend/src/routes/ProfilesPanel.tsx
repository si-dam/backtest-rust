import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { getPresetProfiles } from "../lib/api";

function buildProfileParams() {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * 5);
  return new URLSearchParams({
    symbol_contract: "NQH6",
    start: start.toISOString(),
    end: end.toISOString(),
    preset: "day",
    timezone: "America/New_York",
    metric: "volume",
    tick_aggregation: "1",
    value_area_enabled: "true",
    value_area_percent: "70",
    max_segments: "5",
  });
}

export default function ProfilesPanel() {
  const profileParams = useMemo(buildProfileParams, []);
  const profilesQuery = useQuery({
    queryKey: ["preset-profiles"],
    queryFn: () => getPresetProfiles(profileParams),
  });

  return (
    <section className="panel-grid">
      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Profiles</p>
            <h2>Preset volume profiles</h2>
          </div>
          <span className="pill">{profilesQuery.data?.profiles.length ?? 0} segments</span>
        </div>
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
              </div>
            </div>
          ))}
        </div>
      </article>
    </section>
  );
}
