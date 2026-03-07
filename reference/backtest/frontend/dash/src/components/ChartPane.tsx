import { useEffect, useMemo, useRef, useState } from 'react';
import { createChart, type IChartApi, type ISeriesApi, type CandlestickData, type HistogramData, type LineData, type UTCTimestamp } from 'lightweight-charts';
import { getBars, getLargeOrders, getVwapPreset, type ChartBar } from '../lib/api';

type Props = {
  paneId: 'a' | 'b';
  active: boolean;
  symbol: string;
  timeframe: string;
  barType: 'time' | 'tick' | 'volume' | 'range';
  barSize: number;
  showVolume: boolean;
  showLarge: boolean;
  largeThreshold: number;
  showVwap: boolean;
  showOrb: boolean;
  startIso: string;
  endIso: string;
};

function toUtc(isoTs: string): UTCTimestamp {
  return Math.floor(new Date(isoTs).getTime() / 1000) as UTCTimestamp;
}

function toCandles(rows: ChartBar[]): CandlestickData[] {
  return rows.map((r) => ({
    time: toUtc(r.ts),
    open: r.open,
    high: r.high,
    low: r.low,
    close: r.close,
  }));
}

function buildOrbLines(rows: ChartBar[], minutes = 15): Array<{ value: number; color: string; title: string }> {
  if (!rows.length) return [];
  const start = new Date(rows[0].ts).getTime();
  const end = start + minutes * 60 * 1000;
  const orb = rows.filter((r) => {
    const ts = new Date(r.ts).getTime();
    return ts >= start && ts <= end;
  });
  if (!orb.length) return [];
  const high = Math.max(...orb.map((r) => r.high));
  const low = Math.min(...orb.map((r) => r.low));
  return [
    { value: high, color: '#ffca28', title: 'ORB High' },
    { value: low, color: '#29b6f6', title: 'ORB Low' },
  ];
}

export function ChartPane(props: Props) {
  const elRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const vwapRef = useRef<ISeriesApi<'Line'> | null>(null);
  const orbLinesRef = useRef<ReturnType<ISeriesApi<'Candlestick'>['createPriceLine']>[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [noBars, setNoBars] = useState(false);

  const storagePrefix = useMemo(() => `dash-react.chart.${props.paneId}.v1`, [props.paneId]);

  useEffect(() => {
    const node = elRef.current;
    if (!node) return;
    const chart = createChart(node, {
      layout: { background: { color: '#1f2a37' }, textColor: '#d1d4dc' },
      grid: { vertLines: { color: '#2a2e39' }, horzLines: { color: '#2a2e39' } },
      rightPriceScale: { visible: true, borderVisible: false },
      timeScale: { borderVisible: false, timeVisible: true },
      crosshair: { mode: 1 },
      width: node.clientWidth,
      height: node.clientHeight,
    });
    chartRef.current = chart;
    candleRef.current = chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
      borderVisible: false,
    });

    const resize = new ResizeObserver(() => {
      if (!elRef.current || !chartRef.current) return;
      chartRef.current.applyOptions({ width: elRef.current.clientWidth, height: elRef.current.clientHeight });
    });
    resize.observe(node);

    return () => {
      resize.disconnect();
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
      vwapRef.current = null;
    };
  }, []);

  useEffect(() => {
    async function run() {
      if (!chartRef.current || !candleRef.current || !props.symbol) return;
      setLoading(true);
      setError('');
      try {
        const bars = await getBars({
          symbol_contract: props.symbol,
          timeframe: props.timeframe,
          bar_type: props.barType,
          bar_size: props.barSize,
          start: props.startIso,
          end: props.endIso,
        });

        if (!bars.length) {
          setNoBars(true);
          candleRef.current.setData([]);
          if (volumeRef.current) volumeRef.current.setData([]);
          if (vwapRef.current) vwapRef.current.setData([]);
          candleRef.current.setMarkers([]);
          return;
        }

        setNoBars(false);

        candleRef.current.setData(toCandles(bars));
        chartRef.current.timeScale().fitContent();

        if (props.showVolume) {
          if (!volumeRef.current) {
            volumeRef.current = chartRef.current.addHistogramSeries({ priceScaleId: '', priceLineVisible: false, lastValueVisible: false });
          }
          const volumeRows: HistogramData[] = bars.map((r) => ({
            time: toUtc(r.ts),
            value: r.volume ?? 0,
            color: (r.close >= r.open) ? 'rgba(38,166,154,0.4)' : 'rgba(239,83,80,0.4)',
          }));
          volumeRef.current?.setData(volumeRows);
        } else if (volumeRef.current) {
          volumeRef.current.setData([]);
        }

        if (props.showLarge) {
          const large = await getLargeOrders({ symbol_contract: props.symbol, start: props.startIso, end: props.endIso, fixed_threshold: props.largeThreshold });
          candleRef.current.setMarkers(
            large.slice(0, 1000).map((row) => ({
              time: toUtc(row.ts),
              position: 'inBar',
              shape: 'circle',
              size: 1,
              color: row.side === 'sell' ? '#ff1744' : '#00c853',
              text: String(row.qty ?? ''),
            }))
          );
        } else {
          candleRef.current.setMarkers([]);
        }

        if (props.showVwap) {
          const vwap = await getVwapPreset({ symbol_contract: props.symbol, start: props.startIso, end: props.endIso, timezone: 'America/New_York', preset: 'day' });
          if (!vwapRef.current) {
            vwapRef.current = chartRef.current.addLineSeries({ color: '#ffca28', lineWidth: 2 });
          }
          const points: LineData[] = (vwap.segments ?? []).flatMap((s) =>
            (s.points ?? []).map((p) => ({ time: toUtc(p.ts), value: p.vwap }))
          );
          vwapRef.current?.setData(points);
        } else if (vwapRef.current) {
          vwapRef.current.setData([]);
        }

        for (const line of orbLinesRef.current) {
          candleRef.current.removePriceLine(line);
        }
        orbLinesRef.current = [];
        if (props.showOrb) {
          for (const line of buildOrbLines(bars)) {
            const created = candleRef.current.createPriceLine({
              price: line.value,
              color: line.color,
              lineWidth: 1,
              lineStyle: 2,
              title: line.title,
              axisLabelVisible: true,
            });
            orbLinesRef.current.push(created);
          }
        }

        localStorage.setItem(`${storagePrefix}.last-symbol`, props.symbol);
      } catch (err) {
        setError((err as Error).message);
        setNoBars(false);
      } finally {
        setLoading(false);
      }
    }
    void run();
  }, [props.symbol, props.timeframe, props.barType, props.barSize, props.showVolume, props.showLarge, props.largeThreshold, props.showVwap, props.showOrb, props.startIso, props.endIso, storagePrefix]);

  return (
    <div className={props.active ? 'chart-pane active' : 'chart-pane'}>
      <div className="chart-pane-header">Pane {props.paneId.toUpperCase()} {loading && <span>Loading...</span>} {error && <span className="status">{error}</span>}</div>
      <div ref={elRef} className="chart-pane-canvas" />
      {noBars && <div className="chart-pane-empty">No bars in selected range. Adjust time window or symbol.</div>}
    </div>
  );
}
