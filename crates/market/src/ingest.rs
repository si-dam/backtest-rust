use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::{DateTime, NaiveDateTime, TimeZone, Timelike, Utc};
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

pub fn parse_market_data_file(
    path: &Path,
    dataset_timezone: Tz,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<ParsedMarketData> {
    let bytes = fs::read(path).with_context(|| format!("unable to open {}", path.display()))?;
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .delimiter(detect_delimiter(&bytes))
        .from_reader(bytes.as_slice());

    let headers = reader.headers().context("missing headers")?.clone();
    let lookup = HeaderLookup::from_headers(&headers);
    let mut records = reader.records();
    let first_record = records
        .next()
        .transpose()
        .context("failed to read row 2")?;

    if TickColumns::can_parse(&lookup) {
        let columns = TickColumns::from_lookup(&lookup)?;
        let mut ticks = Vec::new();
        if let Some(record) = first_record.as_ref() {
            ticks.push(
                parse_tick_record(record, &columns, dataset_timezone, fallback_symbol)
                    .with_context(|| "failed to parse tick row 2")?,
            );
        }
        for (row_index, row) in records.enumerate() {
            let line_number = row_index + 3;
            let record = row.with_context(|| format!("failed to read row {line_number}"))?;
            ticks.push(
                parse_tick_record(&record, &columns, dataset_timezone, fallback_symbol)
                    .with_context(|| format!("failed to parse tick row {line_number}"))?,
            );
        }
        return Ok(ParsedMarketData::Ticks(ticks));
    }

    if SierraTickBarColumns::can_parse(&lookup) {
        let columns = SierraTickBarColumns::from_lookup(&lookup)?;
        let should_parse_as_ticks = first_record
            .as_ref()
            .map(|record| sierra_tick_bar_is_subminute(record, &columns, dataset_timezone))
            .transpose()?
            .unwrap_or(false);

        if should_parse_as_ticks {
            let mut ticks = Vec::new();
            if let Some(record) = first_record.as_ref() {
                ticks.push(
                    parse_sierra_tick_bar_record(record, &columns, dataset_timezone, fallback_symbol)
                        .with_context(|| "failed to parse Sierra tick row 2")?,
                );
            }
            for (row_index, row) in records.enumerate() {
                let line_number = row_index + 3;
                let record = row.with_context(|| format!("failed to read row {line_number}"))?;
                ticks.push(
                    parse_sierra_tick_bar_record(&record, &columns, dataset_timezone, fallback_symbol)
                        .with_context(|| format!("failed to parse Sierra tick row {line_number}"))?,
                );
            }
            return Ok(ParsedMarketData::Ticks(ticks));
        }
    }

    let columns = OhlcColumns::from_lookup(&lookup)?;
    let mut bars = Vec::new();
    if let Some(record) = first_record.as_ref() {
        bars.push(
            parse_ohlc_record(record, &columns, dataset_timezone, fallback_symbol)
                .with_context(|| "failed to parse OHLC row 2")?,
        );
    }
    for (row_index, row) in records.enumerate() {
        let line_number = row_index + 3;
        let record = row.with_context(|| format!("failed to read row {line_number}"))?;
        bars.push(
            parse_ohlc_record(&record, &columns, dataset_timezone, fallback_symbol)
                .with_context(|| format!("failed to parse OHLC row {line_number}"))?,
        );
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
        aliases
            .iter()
            .find_map(|alias| self.index_by_name.get(&normalize_header(alias)).copied())
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
        (lookup
            .find(&["timestamp", "date time", "datetime", "date_time", "ts"])
            .is_some()
            || (lookup.find(&["date"]).is_some() && lookup.find(&["time"]).is_some()))
            && lookup
                .find(&["trade price", "price", "last", "last price"])
                .is_some()
            && lookup
                .find(&["trade size", "size", "volume", "qty", "quantity"])
                .is_some()
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
            bid_price: lookup
                .find(&["bid", "bid price", "bidprice"])
                .context("missing bid price column")?,
            ask_price: lookup
                .find(&["ask", "ask price", "askprice"])
                .context("missing ask price column")?,
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

struct SierraTickBarColumns {
    timestamp: Option<usize>,
    date: Option<usize>,
    time: Option<usize>,
    trade_price: usize,
    trade_size: usize,
    bid_volume: usize,
    ask_volume: usize,
    symbol_contract: Option<usize>,
}

impl SierraTickBarColumns {
    fn can_parse(lookup: &HeaderLookup) -> bool {
        (lookup
            .find(&["timestamp", "date time", "datetime", "date_time", "ts"])
            .is_some()
            || (lookup.find(&["date"]).is_some() && lookup.find(&["time"]).is_some()))
            && lookup.find(&["open"]).is_some()
            && lookup.find(&["high"]).is_some()
            && lookup.find(&["low"]).is_some()
            && lookup.find(&["last", "close", "last price"]).is_some()
            && lookup.find(&["volume", "vol"]).is_some()
            && lookup
                .find(&[
                    "numberoftrades",
                    "number of trades",
                    "trades",
                    "trade_count",
                    "trade count",
                ])
                .is_some()
            && lookup.find(&["bidvolume", "bid volume"]).is_some()
            && lookup.find(&["askvolume", "ask volume"]).is_some()
            && lookup.find(&["bid", "bid price", "bidprice"]).is_none()
            && lookup.find(&["ask", "ask price", "askprice"]).is_none()
    }

    fn from_lookup(lookup: &HeaderLookup) -> anyhow::Result<Self> {
        Ok(Self {
            timestamp: lookup.find(&["timestamp", "date time", "datetime", "date_time", "ts"]),
            date: lookup.find(&["date"]),
            time: lookup.find(&["time"]),
            trade_price: lookup
                .find(&["last", "close", "last price"])
                .context("missing trade price column")?,
            trade_size: lookup
                .find(&["volume", "vol"])
                .context("missing trade size column")?,
            bid_volume: lookup
                .find(&["bidvolume", "bid volume"])
                .context("missing bid volume column")?,
            ask_volume: lookup
                .find(&["askvolume", "ask volume"])
                .context("missing ask volume column")?,
            symbol_contract: lookup.find(&["symbol", "symbol_contract", "contract"]),
        })
    }
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
            close: lookup
                .find(&["last", "close", "last price"])
                .context("missing close column")?,
            volume: lookup
                .find(&["volume", "vol"])
                .context("missing volume column")?,
            trade_count: lookup
                .find(&[
                    "numberoftrades",
                    "number of trades",
                    "trades",
                    "trade_count",
                    "trade count",
                ])
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
    let ts = parse_record_timestamp(
        record,
        columns.timestamp,
        columns.date,
        columns.time,
        dataset_timezone,
    )?;
    let symbol_contract = record
        .get(columns.symbol_contract.unwrap_or(usize::MAX))
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| fallback_symbol.map(ToString::to_string))
        .unwrap_or_else(|| "UNKNOWN".to_string());

    Ok(CanonicalTick::new(
        ts,
        &symbol_contract,
        parse_f64(record, columns.trade_price, "trade price")?,
        parse_f64(record, columns.trade_size, "trade size")?,
        Some(parse_f64(record, columns.bid_price, "bid price")?),
        Some(parse_f64(record, columns.ask_price, "ask price")?),
    ))
}

fn parse_ohlc_record(
    record: &StringRecord,
    columns: &OhlcColumns,
    dataset_timezone: Tz,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<TimeBarRow> {
    let ts = parse_record_timestamp(
        record,
        columns.timestamp,
        columns.date,
        columns.time,
        dataset_timezone,
    )?;
    if ts.second() != 0 || ts.timestamp_subsec_nanos() != 0 {
        anyhow::bail!("OHLC timestamps must be minute-aligned");
    }
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
        open: parse_f64(record, columns.open, "open")?,
        high: parse_f64(record, columns.high, "high")?,
        low: parse_f64(record, columns.low, "low")?,
        close: parse_f64(record, columns.close, "close")?,
        volume: parse_f64(record, columns.volume, "volume")?,
        trade_count: parse_f64(record, columns.trade_count, "trade_count")? as u64,
    };

    Ok(TimeBarRow::from_bar(trading_bar))
}

fn parse_sierra_tick_bar_record(
    record: &StringRecord,
    columns: &SierraTickBarColumns,
    dataset_timezone: Tz,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<CanonicalTick> {
    let ts = parse_record_timestamp(
        record,
        columns.timestamp,
        columns.date,
        columns.time,
        dataset_timezone,
    )?;
    let symbol_contract = record
        .get(columns.symbol_contract.unwrap_or(usize::MAX))
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| fallback_symbol.map(ToString::to_string))
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let trade_price = parse_f64(record, columns.trade_price, "trade price")?;
    let trade_size = parse_f64(record, columns.trade_size, "trade size")?;
    let bid_volume = parse_f64(record, columns.bid_volume, "bid volume")?;
    let ask_volume = parse_f64(record, columns.ask_volume, "ask volume")?;
    let epsilon = 1e-9_f64;
    let (bid_price, ask_price) = if ask_volume > 0.0 && bid_volume <= 0.0 {
        (Some(trade_price - epsilon), Some(trade_price))
    } else if bid_volume > 0.0 && ask_volume <= 0.0 {
        (Some(trade_price), Some(trade_price + epsilon))
    } else {
        (None, None)
    };

    Ok(CanonicalTick::new(
        ts,
        &symbol_contract,
        trade_price,
        trade_size,
        bid_price,
        ask_price,
    ))
}

fn sierra_tick_bar_is_subminute(
    record: &StringRecord,
    columns: &SierraTickBarColumns,
    dataset_timezone: Tz,
) -> anyhow::Result<bool> {
    let ts = parse_record_timestamp(
        record,
        columns.timestamp,
        columns.date,
        columns.time,
        dataset_timezone,
    )?;
    Ok(ts.second() != 0 || ts.timestamp_subsec_nanos() != 0)
}

fn parse_record_timestamp(
    record: &StringRecord,
    timestamp_column: Option<usize>,
    date_column: Option<usize>,
    time_column: Option<usize>,
    dataset_timezone: Tz,
) -> anyhow::Result<DateTime<Utc>> {
    if let Some(index) = timestamp_column {
        return parse_datetime_field(record, index, "timestamp", dataset_timezone);
    }

    let date_value = parse_text_field(record, date_column.context("missing date column")?, "date")?;
    let time_value = parse_text_field(record, time_column.context("missing time column")?, "time")?;
    parse_datetime_value(&format!("{date_value} {time_value}"), dataset_timezone).with_context(
        || format!("invalid timestamp from date/time fields: {date_value} {time_value}"),
    )
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

fn parse_datetime_field(
    record: &StringRecord,
    index: usize,
    field_name: &str,
    dataset_timezone: Tz,
) -> anyhow::Result<DateTime<Utc>> {
    let value = parse_text_field(record, index, field_name)?;
    parse_datetime_value(value, dataset_timezone)
        .with_context(|| format!("invalid {field_name}: {value}"))
}

fn parse_text_field<'a>(
    record: &'a StringRecord,
    index: usize,
    field_name: &str,
) -> anyhow::Result<&'a str> {
    let value = record
        .get(index)
        .with_context(|| format!("missing {field_name} value at column {}", index + 1))?
        .trim();
    if value.is_empty() {
        anyhow::bail!("{field_name} is empty at column {}", index + 1);
    }
    Ok(value)
}

fn parse_f64(record: &StringRecord, index: usize, field_name: &str) -> anyhow::Result<f64> {
    let value = parse_text_field(record, index, field_name)?;
    value
        .parse::<f64>()
        .with_context(|| format!("invalid {field_name}: {value}"))
}

fn normalize_header(value: &str) -> String {
    value.trim().to_lowercase().replace([' ', '-', '.'], "_")
}

fn detect_delimiter(bytes: &[u8]) -> u8 {
    let first_line = bytes
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    if first_line.contains(&b'\t') {
        b'\t'
    } else {
        b','
    }
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
    fn parses_tab_delimited_txt_tick_schema() {
        let path = write_fixture(
            "backtest-rust-ticks.txt",
            "date\ttime\ttrade price\ttrade size\tbid price\task price\tsymbol\n2026-03-01\t09:30:00\t100.0\t2\t99.75\t100.0\tNQH6\n",
        );
        let parsed = parse_market_data_file(&path, New_York, None).unwrap();
        match parsed {
            ParsedMarketData::Ticks(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].symbol_contract, "NQH6");
                assert_eq!(rows[0].trade_price, 100.0);
            }
            ParsedMarketData::Ohlc1m(_) => panic!("expected tick rows"),
        }
    }

    #[test]
    fn parses_sierra_datetime_tick_schema() {
        let path = write_fixture(
            "backtest-rust-sierra-ticks.txt",
            "Date Time,Price,Volume,Bid,Ask\n2026-03-01 09:30:00,22000.25,3,22000.0,22000.25\n",
        );
        let parsed = parse_market_data_file(&path, New_York, Some("NQH6")).unwrap();
        match parsed {
            ParsedMarketData::Ticks(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].symbol_contract, "NQH6");
                assert_eq!(rows[0].trade_price, 22000.25);
                assert_eq!(rows[0].trade_size, 3.0);
                assert_eq!(rows[0].bid_price, Some(22000.0));
                assert_eq!(rows[0].ask_price, Some(22000.25));
            }
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

    #[test]
    fn parses_sierra_ohlc_with_extra_columns() {
        let path = write_fixture(
            "backtest-rust-sierra-ohlc.txt",
            concat!(
                "Date,Time,Open,High,Low,Last,Volume,NumberOfTrades,BidVolume,AskVolume\n",
                "2026/03/01,09:30:00,100,101,99,100.5,10,4,5,5\n",
            ),
        );
        let parsed = parse_market_data_file(&path, New_York, Some("NQH6")).unwrap();
        match parsed {
            ParsedMarketData::Ohlc1m(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].timeframe, "1m");
                assert_eq!(rows[0].symbol_contract, "NQH6");
                assert_eq!(rows[0].trade_count, 4);
                assert_eq!(rows[0].close, 100.5);
            }
            ParsedMarketData::Ticks(_) => panic!("expected ohlc rows"),
        }
    }

    #[test]
    fn parses_sierra_scid_bar_data_ticks_as_ticks() {
        let path = write_fixture(
            "backtest-rust-sierra-scid-bar-data-ticks.txt",
            concat!(
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume\n",
                "2025/6/24, 11:17:10.810, 22770.25, 22770.25, 22770.25, 22770.25, 1, 1, 0, 1\n",
                "2025/6/24, 11:17:37.775, 22773.25, 22773.25, 22773.25, 22773.25, 3, 1, 3, 0\n",
            ),
        );
        let parsed = parse_market_data_file(&path, New_York, Some("NQH6")).unwrap();
        match parsed {
            ParsedMarketData::Ticks(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].trade_price, 22770.25);
                assert_eq!(rows[0].trade_size, 1.0);
                assert!(rows[0].ask_price.is_some());
                assert_eq!(rows[1].trade_size, 3.0);
                assert!(rows[1].bid_price.is_some());
            }
            ParsedMarketData::Ohlc1m(_) => panic!("expected tick rows"),
        }
    }

    #[test]
    fn reports_bad_numeric_field_with_line_context() {
        let path = write_fixture(
            "backtest-rust-bad-number.csv",
            "timestamp,trade price,trade size,bid price,ask price\n2026-03-01T14:30:00Z,not-a-number,2,99.75,100.0\n",
        );
        let error = parse_market_data_file(&path, New_York, Some("NQH6")).unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("failed to parse tick row 2"));
        assert!(message.contains("invalid trade price: not-a-number"));
    }

    #[test]
    fn reports_bad_timestamp_with_line_context() {
        let path = write_fixture(
            "backtest-rust-bad-ts.txt",
            "date\ttime\ttrade price\ttrade size\tbid price\task price\tsymbol\n2026-03-01\tbad-time\t100.0\t2\t99.75\t100.0\tNQH6\n",
        );
        let error = parse_market_data_file(&path, New_York, None).unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("failed to parse tick row 2"));
        assert!(message.contains("invalid timestamp from date/time fields: 2026-03-01 bad-time"));
    }

    #[test]
    fn rejects_non_minute_aligned_ohlc_timestamps() {
        let path = write_fixture(
            "backtest-rust-bad-ohlc-ts.txt",
            "Date,Time,Open,High,Low,Last,Volume,NumberOfTrades\n2026/03/01,09:30:00.500,100,101,99,100.5,10,4\n",
        );
        let error = parse_market_data_file(&path, New_York, Some("NQH6")).unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("failed to parse OHLC row 2"));
        assert!(message.contains("OHLC timestamps must be minute-aligned"));
    }
}
