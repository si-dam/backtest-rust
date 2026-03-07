use chrono_tz::America::New_York;
use market::{
    build_non_time_bars_from_ticks, build_profiles_for_ticks, build_time_bars_from_ticks, CanonicalTick, NonTimeBarRow,
    TimeBarRow,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct GoldenExpected {
    time_bars_1m: Vec<TimeBarRow>,
    tick_bars_3: Vec<NonTimeBarRow>,
    profile_count: usize,
    profiles: Vec<GoldenProfile>,
}

#[derive(Debug, Deserialize)]
struct GoldenProfile {
    preset: String,
    metric: String,
    label: String,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    total_value: f64,
    max_value: f64,
    value_area_enabled: bool,
    value_area_poc: Option<f64>,
    value_area_low: Option<f64>,
    value_area_high: Option<f64>,
    value_area_volume: f64,
    levels: Vec<GoldenLevel>,
}

#[derive(Debug, Deserialize)]
struct GoldenLevel {
    price_level: f64,
    value: f64,
}

#[test]
fn matches_golden_bars_and_profiles() {
    let ticks: Vec<CanonicalTick> = serde_json::from_str(include_str!("fixtures/golden_ticks.json")).unwrap();
    let expected: GoldenExpected = serde_json::from_str(include_str!("fixtures/golden_expected.json")).unwrap();

    let actual_time = build_time_bars_from_ticks(&ticks, "NQH6", "1m", New_York).unwrap();
    assert_eq!(actual_time.len(), expected.time_bars_1m.len());
    assert_eq!(serde_json::to_value(actual_time).unwrap(), serde_json::to_value(expected.time_bars_1m).unwrap());

    let actual_tick = build_non_time_bars_from_ticks(&ticks, "NQH6", "tick", 3, 0.25, New_York).unwrap();
    assert_eq!(actual_tick.len(), expected.tick_bars_3.len());
    assert_eq!(serde_json::to_value(actual_tick).unwrap(), serde_json::to_value(expected.tick_bars_3).unwrap());

    let profiles = build_profiles_for_ticks("NQH6", &ticks, New_York, 0.25);
    assert_eq!(profiles.len(), expected.profile_count);

    for expected_profile in expected.profiles {
        let actual = profiles
            .iter()
            .find(|profile| {
                profile.segment.preset == expected_profile.preset
                    && profile.segment.metric == expected_profile.metric
                    && profile.segment.label == expected_profile.label
            })
            .unwrap_or_else(|| panic!("missing profile {} {}", expected_profile.preset, expected_profile.metric));

        assert_eq!(actual.segment.segment_start, expected_profile.start);
        assert_eq!(actual.segment.segment_end, expected_profile.end);
        assert_close(actual.segment.total_value, expected_profile.total_value);
        assert_close(actual.segment.max_value, expected_profile.max_value);
        assert_eq!(actual.segment.value_area_enabled, expected_profile.value_area_enabled);
        assert_option_close(actual.segment.value_area_poc, expected_profile.value_area_poc);
        assert_option_close(actual.segment.value_area_low, expected_profile.value_area_low);
        assert_option_close(actual.segment.value_area_high, expected_profile.value_area_high);
        assert_close(actual.segment.value_area_volume, expected_profile.value_area_volume);

        let actual_levels = actual
            .levels
            .iter()
            .map(|level| GoldenLevel {
                price_level: level.price_level,
                value: if expected_profile.metric == "delta" {
                    level.delta
                } else {
                    level.total_volume
                },
            })
            .collect::<Vec<_>>();

        assert_eq!(actual_levels.len(), expected_profile.levels.len());
        for (actual_level, expected_level) in actual_levels.iter().zip(expected_profile.levels.iter()) {
            assert_close(actual_level.price_level, expected_level.price_level);
            assert_close(actual_level.value, expected_level.value);
        }
    }
}

fn assert_close(left: f64, right: f64) {
    let diff = (left - right).abs();
    assert!(diff < 1e-9, "left={left} right={right} diff={diff}");
}

fn assert_option_close(left: Option<f64>, right: Option<f64>) {
    match (left, right) {
        (Some(left), Some(right)) => assert_close(left, right),
        (None, None) => {}
        (left, right) => panic!("left={left:?} right={right:?}"),
    }
}
