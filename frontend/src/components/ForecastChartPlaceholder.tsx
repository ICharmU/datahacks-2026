import { LineChart, Line, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts"
import type { TimePoint } from "../types"

export function ForecastChartPlaceholder({ data, title = "Risk forecast" }: { data: TimePoint[]; title?: string }) {
  const chartData = data.map((d) => ({ ...d, label: new Date(d.t).toLocaleTimeString([], { hour: "numeric" }) }))

  return (
    <div className="panel">
      <div className="panel__title">{title}</div>
      <div className="chart-wrap">
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="label" />
            <YAxis domain={[0, 1]} />
            <Tooltip />
            <Line type="monotone" dataKey="value" strokeWidth={3} dot />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
