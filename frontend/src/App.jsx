import React, { useMemo } from "react";
import LiveChart from "./components/LiveChart.jsx";
import { useLiveStream } from "./hooks/useLiveStream.js";

const STATUS_LABEL = {
  connecting: "connecting…",
  live: "live",
  reconnecting: "reconnecting…",
};

const STATUS_CLASS = {
  connecting: "status",
  live: "status status--ok",
  reconnecting: "status status--down",
};

export default function App() {
  const { series, status } = useLiveStream();
  const symbols = useMemo(() => Object.keys(series).sort(), [series]);

  return (
    <div className="app">
      <header className="app__header">
        <h1>Real-Time Analytics Engine</h1>
        <span className={STATUS_CLASS[status] ?? "status"}>
          <span className="status__dot" />
          {STATUS_LABEL[status] ?? status}
        </span>
      </header>

      {symbols.length === 0 ? (
        <div className="empty">Waiting for the first snapshot…</div>
      ) : (
        <div className="grid">
          {symbols.map((sym) => (
            <LiveChart key={sym} symbol={sym} points={series[sym] ?? []} />
          ))}
        </div>
      )}
    </div>
  );
}
