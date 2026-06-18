import { useEffect, useRef, useState } from "react";

const BUFFER_SECONDS = 60;
// Reconnect with exponential backoff capped at 5s so a transient consumer
// restart doesn't leave the UI sitting silent for minutes.
const RECONNECT_MIN_MS = 500;
const RECONNECT_MAX_MS = 5000;

function wsUrl() {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws`;
}

/**
 * Opens a WebSocket to the consumer's broadcast hub and keeps a rolling
 * `BUFFER_SECONDS` window of per-symbol points. Returns:
 *   { series: { [symbol]: [{ ts, price, sample_count }] }, status }.
 */
export function useLiveStream() {
  const [series, setSeries] = useState({});
  const [status, setStatus] = useState("connecting");
  const seriesRef = useRef({});
  const wsRef = useRef(null);
  const backoffRef = useRef(RECONNECT_MIN_MS);
  const cancelledRef = useRef(false);

  useEffect(() => {
    cancelledRef.current = false;

    const connect = () => {
      if (cancelledRef.current) return;
      const ws = new WebSocket(wsUrl());
      wsRef.current = ws;
      setStatus("connecting");

      ws.onopen = () => {
        backoffRef.current = RECONNECT_MIN_MS;
        setStatus("live");
      };

      ws.onmessage = (event) => {
        let parsed;
        try {
          parsed = JSON.parse(event.data);
        } catch {
          return;
        }
        if (parsed.type !== "snapshot" || !Array.isArray(parsed.items)) return;

        const next = { ...seriesRef.current };
        const cutoff = Date.now() / 1000 - BUFFER_SECONDS;
        for (const item of parsed.items) {
          const sym = item.symbol;
          const point = {
            ts: item.ts,
            price: item.avg_price,
            sample_count: item.sample_count,
          };
          const prev = next[sym] ?? [];
          const merged = [...prev, point].filter((p) => p.ts >= cutoff);
          next[sym] = merged;
        }
        seriesRef.current = next;
        setSeries(next);
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (cancelledRef.current) return;
        setStatus("reconnecting");
        const delay = backoffRef.current;
        backoffRef.current = Math.min(delay * 2, RECONNECT_MAX_MS);
        setTimeout(connect, delay);
      };

      ws.onerror = () => {
        // onclose will follow; nothing more to do here.
      };
    };

    connect();

    return () => {
      cancelledRef.current = true;
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch {
          /* ignore */
        }
        wsRef.current = null;
      }
    };
  }, []);

  return { series, status };
}
