use std::cmp::Ordering;

use super::{ProfileLevelValue, ValueAreaSummary};

pub fn bucket_price(price: f64, bucket_size: f64) -> f64 {
    if bucket_size <= 0.0 {
        return price;
    }
    (price / bucket_size).floor() * bucket_size
}

pub fn classify_side(trade_price: f64, bid_price: Option<f64>, ask_price: Option<f64>) -> f64 {
    match (bid_price, ask_price) {
        (Some(bid), Some(ask)) if ask > bid => {
            let midpoint = bid + ((ask - bid) / 2.0);
            if trade_price >= ask {
                1.0
            } else if trade_price <= bid {
                -1.0
            } else if trade_price > midpoint {
                1.0
            } else if trade_price < midpoint {
                -1.0
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

pub fn build_value_area(levels: &[ProfileLevelValue], percent: f64) -> ValueAreaSummary {
    if levels.is_empty() {
        return ValueAreaSummary::disabled(percent);
    }

    let total_volume = levels.iter().map(|row| row.value.max(0.0)).sum::<f64>();
    if total_volume <= 0.0 {
        return ValueAreaSummary::disabled(percent);
    }

    let target = total_volume * (percent / 100.0);
    let poc_index = levels
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.value.partial_cmp(&right.value).unwrap_or(Ordering::Equal))
        .map(|(index, _)| index)
        .unwrap_or(0);

    let mut low = poc_index;
    let mut high = poc_index;
    let mut collected = levels[poc_index].value.max(0.0);

    while collected < target && (low > 0 || high + 1 < levels.len()) {
        let left_index = low.checked_sub(1);
        let right_index = (high + 1 < levels.len()).then_some(high + 1);
        let left_value = left_index.map(|index| levels[index].value).unwrap_or(-1.0);
        let right_value = right_index.map(|index| levels[index].value).unwrap_or(-1.0);

        if right_value > left_value {
            if let Some(index) = right_index {
                high = index;
                collected += levels[index].value.max(0.0);
            }
        } else if let Some(index) = left_index {
            low = index;
            collected += levels[index].value.max(0.0);
        } else if let Some(index) = right_index {
            high = index;
            collected += levels[index].value.max(0.0);
        } else {
            break;
        }
    }

    ValueAreaSummary {
        enabled: true,
        percent,
        poc: Some(levels[poc_index].price_level),
        low: Some(levels[low].price_level),
        high: Some(levels[high].price_level),
        volume: collected,
    }
}
