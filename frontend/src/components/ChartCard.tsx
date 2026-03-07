import { useEffect, useRef } from "react";
import { CandlestickSeries, ColorType, createChart } from "lightweight-charts";
import type { UTCTimestamp } from "lightweight-charts";
import type { BarRecord } from "../lib/api";

interface ChartCardProps {
  bars: BarRecord[];
}

export default function ChartCard({ bars }: ChartCardProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "#f4efe4" },
        textColor: "#2a2724",
      },
      grid: {
        vertLines: { color: "#d6cfbf" },
        horzLines: { color: "#d6cfbf" },
      },
      timeScale: {
        borderColor: "#9f937c",
      },
      rightPriceScale: {
        borderColor: "#9f937c",
      },
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#1f7a5c",
      borderUpColor: "#1f7a5c",
      wickUpColor: "#1f7a5c",
      downColor: "#9a3412",
      borderDownColor: "#9a3412",
      wickDownColor: "#9a3412",
    });

    series.setData(
      bars.map((bar) => ({
        time: Math.floor(new Date(bar.ts).getTime() / 1000) as UTCTimestamp,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      })),
    );
    chart.timeScale().fitContent();

    return () => chart.remove();
  }, [bars]);

  return <div className="chart-card" ref={containerRef} />;
}
