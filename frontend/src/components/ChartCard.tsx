import { useEffect, useRef } from "react";
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  createChart,
  createSeriesMarkers,
} from "lightweight-charts";
import type {
  IChartApi,
  ISeriesApi,
  ISeriesMarkersPluginApi,
  SeriesMarker,
  Time,
  UTCTimestamp,
} from "lightweight-charts";
import type { BarRecord, LargeOrderRecord } from "../lib/api";

interface ChartCardProps {
  bars: BarRecord[];
  largeOrders: LargeOrderRecord[];
  showLargeOrders: boolean;
  showVolume: boolean;
}

function toUtcTimestamp(value: string): UTCTimestamp {
  return Math.floor(new Date(value).getTime() / 1000) as UTCTimestamp;
}

function buildLargeOrderMarkers(
  bars: BarRecord[],
  largeOrders: LargeOrderRecord[],
): SeriesMarker<Time>[] {
  if (!bars.length || !largeOrders.length) {
    return [];
  }

  const barTimes = bars.map((bar) => new Date(bar.ts).getTime());
  const markers: SeriesMarker<Time>[] = [];
  let cursor = 0;

  for (const order of largeOrders) {
    const orderTime = new Date(order.ts).getTime();
    while (cursor + 1 < barTimes.length && barTimes[cursor + 1] <= orderTime) {
      cursor += 1;
    }

    if (barTimes[cursor] > orderTime) {
      continue;
    }

    markers.push({
      time: toUtcTimestamp(bars[cursor].ts),
      position: order.side === "sell" ? "aboveBar" : "belowBar",
      shape: "circle",
      color: order.side === "sell" ? "#ff6b6b" : "#4ade80",
      text: String(Math.round(order.trade_size)),
    });
  }

  return markers.slice(-400);
}

export default function ChartCard({
  bars,
  largeOrders,
  showLargeOrders,
  showVolume,
}: ChartCardProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const markersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    const chart = createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight,
      layout: {
        background: { type: ColorType.Solid, color: "#111925" },
        textColor: "#d5deeb",
      },
      grid: {
        vertLines: { color: "rgba(115, 143, 176, 0.12)" },
        horzLines: { color: "rgba(115, 143, 176, 0.14)" },
      },
      crosshair: {
        mode: CrosshairMode.MagnetOHLC,
      },
      timeScale: {
        borderColor: "rgba(115, 143, 176, 0.28)",
        timeVisible: true,
      },
      rightPriceScale: {
        borderColor: "rgba(115, 143, 176, 0.28)",
      },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#34d399",
      borderUpColor: "#34d399",
      wickUpColor: "#34d399",
      downColor: "#f97316",
      borderDownColor: "#f97316",
      wickDownColor: "#f97316",
    });

    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: "rgba(110, 231, 183, 0.35)",
      priceLineVisible: false,
      lastValueVisible: false,
      priceFormat: {
        type: "volume",
      },
      priceScaleId: "",
    });

    chart.priceScale("").applyOptions({
      scaleMargins: {
        top: 0.78,
        bottom: 0,
      },
    });

    chartRef.current = chart;
    candleRef.current = candleSeries;
    volumeRef.current = volumeSeries;
    markersRef.current = createSeriesMarkers(candleSeries, []);

    const resizeObserver = new ResizeObserver(() => {
      if (!containerRef.current || !chartRef.current) {
        return;
      }

      chartRef.current.applyOptions({
        width: containerRef.current.clientWidth,
        height: containerRef.current.clientHeight,
      });
    });

    resizeObserver.observe(container);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
      markersRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!candleRef.current || !chartRef.current) {
      return;
    }

    candleRef.current.setData(
      bars.map((bar) => ({
        time: toUtcTimestamp(bar.ts),
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      })),
    );
    chartRef.current.timeScale().fitContent();
  }, [bars]);

  useEffect(() => {
    if (!volumeRef.current) {
      return;
    }

    if (!showVolume) {
      volumeRef.current.setData([]);
      return;
    }

    volumeRef.current.setData(
      bars.map((bar) => ({
        time: toUtcTimestamp(bar.ts),
        value: bar.volume ?? 0,
        color:
          bar.close >= bar.open ? "rgba(52, 211, 153, 0.35)" : "rgba(249, 115, 22, 0.35)",
      })),
    );
  }, [bars, showVolume]);

  useEffect(() => {
    if (!markersRef.current) {
      return;
    }

    if (!showLargeOrders) {
      markersRef.current.setMarkers([]);
      return;
    }

    markersRef.current.setMarkers(buildLargeOrderMarkers(bars, largeOrders));
  }, [bars, largeOrders, showLargeOrders]);

  return <div className="chart-card" ref={containerRef} />;
}
