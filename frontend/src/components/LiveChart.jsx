import React from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const STROKE = "#6ad8a1";

function formatTime(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour12: false });
}

function formatPrice(p) {
  if (typeof p !== "number") return "—";
  return p.toFixed(2);
}

export default function LiveChart({ symbol, points }) {
  const last = points.length ? points[points.length - 1].price : null;
  return (
    <div className="card">
      <div className="card__head">
        <span className="card__title">{symbol}</span>
        <span className="card__price">{formatPrice(last)}</span>
      </div>
      <div style={{ width: "100%", height: 220 }}>
        <ResponsiveContainer>
          <LineChart data={points} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
            <CartesianGrid stroke="#1f2c45" strokeDasharray="3 3" />
            <XAxis
              dataKey="ts"
              tickFormatter={formatTime}
              minTickGap={40}
              tick={{ fill: "#7e8aa3", fontSize: 11 }}
              stroke="#1f2c45"
            />
            <YAxis
              domain={["dataMin - 0.5", "dataMax + 0.5"]}
              tickFormatter={(v) => v.toFixed(2)}
              tick={{ fill: "#7e8aa3", fontSize: 11 }}
              stroke="#1f2c45"
              width={64}
            />
            <Tooltip
              contentStyle={{
                background: "#0b1220",
                border: "1px solid #1f2c45",
                color: "#e6edf7",
                fontSize: 12,
              }}
              labelFormatter={(v) => formatTime(v)}
              formatter={(value) => [formatPrice(value), "avg"]}
            />
            <Line
              type="monotone"
              dataKey="price"
              stroke={STROKE}
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
