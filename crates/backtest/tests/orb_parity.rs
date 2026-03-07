use backtest::{merge_orb_params, simulate_orb_breakout_strategy, summarize_breakout_trades, OrbStrategyParams, StrategyBar, StrategyTrade};
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct FixtureCase {
    name: String,
    params: Value,
    bars: Vec<StrategyBar>,
    expected_trades: Vec<StrategyTrade>,
    expected_summary: Value,
}

#[test]
fn orb_breakout_matches_reference_fixtures() {
    let cases: Vec<FixtureCase> = serde_json::from_str(include_str!("fixtures/orb_breakout_cases.json")).unwrap();

    for case in cases {
        let params: OrbStrategyParams = merge_orb_params(&case.params, New_York).unwrap();
        let trades = simulate_orb_breakout_strategy(&case.bars, &params).unwrap();
        assert_eq!(trades.len(), case.expected_trades.len(), "trade count mismatch for {}", case.name);

        for (actual, expected) in trades.iter().zip(case.expected_trades.iter()) {
            assert_eq!(actual.session_date, expected.session_date, "session_date mismatch for {}", case.name);
            assert_eq!(actual.timeframe, expected.timeframe, "timeframe mismatch for {}", case.name);
            assert_eq!(actual.ib_minutes, expected.ib_minutes, "ib mismatch for {}", case.name);
            assert_eq!(actual.side, expected.side, "side mismatch for {}", case.name);
            assert_eq!(actual.exit_reason, expected.exit_reason, "exit_reason mismatch for {}", case.name);
            assert_eq!(actual.entry_time, expected.entry_time, "entry_time mismatch for {}", case.name);
            assert_eq!(actual.exit_time, expected.exit_time, "exit_time mismatch for {}", case.name);
            assert_close(actual.entry_price, expected.entry_price, &case.name, "entry_price");
            assert_close(actual.stop_price, expected.stop_price, &case.name, "stop_price");
            assert_close(actual.target_price, expected.target_price, &case.name, "target_price");
            assert_close(actual.exit_price, expected.exit_price, &case.name, "exit_price");
            assert_close(actual.pnl, expected.pnl, &case.name, "pnl");
            assert_close(actual.r_multiple, expected.r_multiple, &case.name, "r_multiple");
        }

        let summary = summarize_breakout_trades(&trades);
        assert_json_close(&summary, &case.expected_summary, &case.name);
    }
}

fn assert_close(left: f64, right: f64, case: &str, field: &str) {
    let diff = (left - right).abs();
    assert!(diff < 1e-9, "{case} {field}: left={left} right={right} diff={diff}");
}

fn assert_json_close(actual: &Value, expected: &Value, case: &str) {
    let actual_obj = actual.as_object().expect("actual summary object");
    let expected_obj = expected.as_object().expect("expected summary object");
    for (key, expected_value) in expected_obj {
        let actual_value = actual_obj.get(key).unwrap_or_else(|| panic!("{case} missing summary key {key}"));
        match (actual_value, expected_value) {
            (Value::Number(left), Value::Number(right)) => {
                let left = left.as_f64().expect("actual number");
                let right = right.as_f64().expect("expected number");
                assert_close(left, right, case, key);
            }
            _ => assert_eq!(actual_value, expected_value, "{case} summary mismatch for {key}"),
        }
    }
}

#[allow(dead_code)]
fn _parse_datetime(value: &str) -> DateTime<Utc> {
    value.parse().unwrap()
}
