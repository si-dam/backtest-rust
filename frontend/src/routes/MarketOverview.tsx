import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import ChartCard from "../components/ChartCard";
import { getBars, getSymbols } from "../lib/api";

function buildBarsParams(symbolContract: string) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * 24);
  return new URLSearchParams({
    symbol_contract: symbolContract,
    timeframe: "1m",
    start: start.toISOString(),
    end: end.toISOString(),
    bar_type: "time",
  });
}

export default function MarketOverview() {
  const symbolsQuery = useQuery({
    queryKey: ["symbols"],
    queryFn: getSymbols,
  });

  const symbolContract = symbolsQuery.data?.symbols[0]?.symbol_contract ?? "NQH6";
  const barsParams = useMemo(() => buildBarsParams(symbolContract), [symbolContract]);

  const barsQuery = useQuery({
    queryKey: ["bars", symbolContract],
    queryFn: () => getBars(barsParams),
  });

  return (
    <section className="panel-grid">
      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Symbols</p>
            <h2>Available symbols</h2>
          </div>
          <span className="pill">{symbolsQuery.data?.symbols.length ?? 0}</span>
        </div>
        <div className="stack">
          {symbolsQuery.isLoading ? <p>Loading symbols…</p> : null}
          {symbolsQuery.isError ? <p>Unable to load symbols.</p> : null}
          {symbolsQuery.data?.symbols.map((symbol) => (
            <div className="list-row" key={symbol.symbol_contract}>
              <strong>{symbol.symbol_contract}</strong>
            </div>
          ))}
        </div>
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Bars</p>
            <h2>{symbolContract} recent candles</h2>
          </div>
          <span className="pill">{barsQuery.data?.bars.length ?? 0} rows</span>
        </div>
        {barsQuery.isError ? <p>Unable to load bars from the Rust API.</p> : null}
        {barsQuery.isLoading ? <p>Loading bars…</p> : null}
        {barsQuery.data?.bars.length ? <ChartCard bars={barsQuery.data.bars} /> : null}
      </article>
    </section>
  );
}
