use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use csv::StringRecord;
use serde::Serialize;

use crate::{CanonicalTick, TimeBarRow, TradingBar, TradingBarKind};

#[derive(Clone, Debug, Serialize)]
pub enum ParsedMarketData {
    Ticks(Vec<CanonicalTick>),
    Ohlc1m(Vec<TimeBarRow>),
}

#[derive(Clone, Debug, Serialize)]
pub struct ParsedFileSummary {
    pub source_path: PathBuf,
    pub schema_kind: String,
    pub symbol_contract: Option<String>,
    pub row_count: usize,
}

pub fn parse_market_data_file(path: &Path, dataset_timezone: Tz, fallback_symbol: Option<&str>) -> anyhow::Result<ParsedMarketData> {
    let file = File::open(path).with_context(|| format!("unable to open {}", path.display()))?;
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(BufReader::new(file));

    let headers = reader.headers().context("missing headers")?.clone();
    let lookup = HeaderLookup::from_headers(&headers);

    if TickColumns::can_parse(&lookup) {
        let columns = TickColumns::from_lookup(&lookup)?;
        let mut ticks = Vec::new();
        for row in reader.records() {
            let record = row?;
            ticks.push(parse_tick_record(&record, &columns, dataset_timezone, fallback_symbol)?);
        }
        return Ok(ParsedMarketData::Ticks(ticks));
    }

    let columns = OhlcColumns::from_lookup(&lookup)?;
    let mut bars = Vec::new();
    for row in reader.records() {
        let record = row?;
        bars.push(parse_ohlc_record(&record, &columns, dataset_timezone, fallback_symbol)?);
    }
    Ok(ParsedMarketData::Ohlc1m(bars))
}

pub fn summarize_parsed_data(path: &Path, parsed: &ParsedMarketData) -> ParsedFileSummary {
    match parsed {
        ParsedMarketData::Ticks(ticks) => ParsedFileSummary {
            source_path: path.to_path_buf(),
            schema_kind: "ticks".to_string(),
            symbol_contract: ticks.first().map(|row| row.symbol_contract.clone()),
            row_count: ticks.len(),
        },
        ParsedMarketData::Ohlc1m(bars) => ParsedFileSummary {
            source_path: path.to_path_buf(),
            schema_kind: "ohlc_1m".to_string(),
            symbol_contract: bars.first().map(|row| row.symbol_contract.clone()),
            row_count: bars.len(),
        },
    }
}

#[derive(Clone)]
struct HeaderLookup {
    index_by_name: HashMap<String, usize>,
}

impl HeaderLookup {
    fn from_headers(headers: &StringRecord) -> Self {
        let mut index_by_name = HashMap::new();
        for (index, header) in headers.iter().enumerate() {
            index_by_name.insert(normalize_header(header), index);
        }
        Self { index_by_name }
    }

    fn find(&self, aliases: &[&str]) -> Option<usize> {
        aliases.iter().find_map(|alias| self.index_by_name.get(&normalize_header(alias)).copied())
    }
}

struct TickColumns {
    timestamp: Option<usize>,
    date: Option<usize>,
    time: Option<usize>,
    trade_price: usize,
    trade_size: usize,
    bid_price: usize,
    ask_price: usize,
    symbol_contract: Option<usize>,
}

impl TickColumns {
    fn can_parse(lookup: &HeaderLookup) -> bool {
        (lookup.find(&["timestamp", "date time", "datetime", "date_time", "ts"]).is_some()
            || (lookup.find(&["date"]).is_some() && lookup.find(&["time"]).is_some()))
            && lookup.find(&["trade price", "price", "last", "last price"]).is_some()
            && lookup.find(&["trade size", "size", "volume", "qty", "quantity"]).is_some()
            && lookup.find(&["bid", "bid price", "bidprice"]).is_some()
            && lookup.find(&["ask", "ask price", "askprice"]).is_some()
    }

    fn from_lookup(lookup: &HeaderLookup) -> anyhow::Result<Self> {
        Ok(Self {
            timestamp: lookup.find(&["timestamp", "date time", "datetime", "date_time", "ts"]),
            date: lookup.find(&["date"]),
            time: lookup.find(&["time"]),
            trade_price: lookup
                .find(&["trade price", "price", "last", "last price"])
                .context("missing trade price column")?,
            trade_size: lookup
                .find(&["trade size", "size", "volume", "qty", "quantity"])
                .context("missing trade size column")?,
            bid_price: lookup.find(&["bid", "bid price", "bidprice"]).context("missing bid price column")?,
            ask_price: lookup.find(&["ask", "ask price", "askprice"]).context("missing ask price column")?,
            symbol_contract: lookup.find(&["symbol", "symbol_contract", "contract"]),
        })
    }
}

struct OhlcColumns {
    timestamp: Option<usize>,
    date: Option<usize>,
    time: Option<usize>,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    volume: usize,
    trade_count: usize,
    symbol_contract: Option<usize>,
}

impl OhlcColumns {
    fn from_lookup(lookup: &HeaderLookup) -> anyhow::Result<Self> {
        Ok(Self {
            timestamp: lookup.find(&["timestamp", "date time", "datetime", "date_time", "ts"]),
            date: lookup.find(&["date"]),
            time: lookup.find(&["time"]),
            open: lookup.find(&["open"]).context("missing open column")?,
            high: lookup.find(&["high"]).context("missing high column")?,
            low: lookup.find(&["low"]).context("missing low column")?,
            close: lookup.find(&["last", "close", "last price"]).context("missing close column")?,
            volume: lookup.find(&["volume", "vol"]).context("missing volume column")?,
            trade_count: lookup
                .find(&["numberoftrades", "number of trades", "trades", "trade_count", "trade count"])
                .context("missing trade_count column")?,
            symbol_contract: lookup.find(&["symbol", "symbol_contract", "contract"]),
        })
    }
}

fn parse_tick_record(
    record: &StringRecord,
    columns: &TickColumns,
    dataset_timezone: Tz,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<CanonicalTick> {
    let ts = parse_record_timestamp(record, columns.timestamp, columns.date, columns.time, dataset_timezone)?;
    let symbol_contract = record
        .get(columns.symbol_contract.unwrap_or(usize::MAX))
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| fallback_symbol.map(ToString::to_string))
        .unwrap_or_else(|| "UNKNOWN".to_string());

    Ok(CanonicalTick::new(
        ts,
        &symbol_contract,
        parse_f64(record, columns.trade_price)?,
        parse_f64(record, columns.trade_size)?,
        Some(parse_f64(record, columns.bid_price)?),
        Some(parse_f64(record, columns.ask_price)?),
    ))
}

fn parse_ohlc_record(
    record: &StringRecord,
    columns: &OhlcColumns,
    dataset_timezone: Tz,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<TimeBarRow> {
    let ts = parse_record_timestamp(record, columns.timestamp, columns.date, columns.time, dataset_timezone)?;
    let symbol_contract = record
        .get(columns.symbol_contract.unwrap_or(usize::MAX))
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| fallback_symbol.map(ToString::to_string))
        .unwrap_or_else(|| "UNKNOWN".to_string());

    let localized = ts.with_timezone(&dataset_timezone);
    let trading_bar = TradingBar {
        ts,
        trading_day: ts.date_naive(),
        session_date: localized.date_naive(),
        symbol_contract,
        kind: TradingBarKind::Time("1m".to_string()),
        open: parse_f64(record, columns.open)?,
        high: parse_f64(record, columns.high)?,
        low: parse_f64(record, columns.low)?,
        close: parse_f64(record, columns.close)?,
        volume: parse_f64(record, columns.volume)?,
        trade_count: parse_f64(record, columns.trade_count)? as u64,
    };

    Ok(TimeBarRow::from_bar(trading_bar))
}

fn parse_record_timestamp(
    record: &StringRecord,
    timestamp_column: Option<usize>,
    date_column: Option<usize>,
    time_column: Option<usize>,
    dataset_timezone: Tz,
) -> anyhow::Result<DateTime<Utc>> {
    if let Some(index) = timestamp_column {
        return parse_datetime_value(record.get(index).unwrap_or_default(), dataset_timezone);
    }

    let date_value = record
        .get(date_column.context("missing date column")?)
        .context("missing date value")?;
    let time_value = record
        .get(time_column.context("missing time column")?)
        .context("missing time value")?;
    parse_datetime_value(&format!("{date_value} {time_value}"), dataset_timezone)
}

fn parse_datetime_value(value: &str, dataset_timezone: Tz) -> anyhow::Result<DateTime<Utc>> {
    let trimmed = value.trim();
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(timestamp.with_timezone(&Utc));
    }

    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y/%m/%d %H:%M:%S%.f",
        "%m/%d/%Y %H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M",
    ] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(trimmed, format) {
            return dataset_timezone
                .from_local_datetime(&parsed)
                .single()
                .map(|value| value.with_timezone(&Utc))
                .context("ambiguous local timestamp");
        }
    }

    anyhow::bail!("unsupported timestamp format: {trimmed}")
}

fn parse_f64(record: &StringRecord, index: usize) -> anyhow::Result<f64> {
    record
        .get(index)
        .context("missing numeric value")?
        .trim()
        .parse::<f64>()
        .with_context(|| "invalid numeric value")
}

fn normalize_header(value: &str) -> String {
    value
        .trim()
        .to_lowercase()
        .replace([' ', '-', '.'], "_")
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use chrono_tz::America::New_York;

    use super::{parse_market_data_file, summarize_parsed_data, ParsedMarketData};

    fn write_fixture(name: &str, contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(name);
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn parses_tick_schema() {
        let path = write_fixture(
            "backtest-rust-ticks.csv",
            "timestamp,trade price,trade size,bid price,ask price\n2026-03-01T14:30:00Z,100.0,2,99.75,100.0\n",
        );
        let parsed = parse_market_data_file(&path, New_York, Some("NQH6")).unwrap();
        let summary = summarize_parsed_data(&path, &parsed);
        assert_eq!(summary.schema_kind, "ticks");
        match parsed {
            ParsedMarketData::Ticks(rows) => assert_eq!(rows[0].symbol_contract, "NQH6"),
            ParsedMarketData::Ohlc1m(_) => panic!("expected tick rows"),
        }
    }

    #[test]
    fn parses_ohlc_schema() {
        let path = write_fixture(
            "backtest-rust-ohlc.csv",
            "date,time,open,high,low,last,volume,number of trades\n2026-03-01,09:30:00,100,101,99,100.5,10,4\n",
        );
        let parsed = parse_market_data_file(&path, New_York, Some("NQH6")).unwrap();
        match parsed {
            ParsedMarketData::Ohlc1m(rows) => assert_eq!(rows[0].timeframe, "1m"),
            ParsedMarketData::Ticks(_) => panic!("expected ohlc rows"),
        }
    }
}
