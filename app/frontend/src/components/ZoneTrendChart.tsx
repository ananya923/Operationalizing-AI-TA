import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from 'recharts'
import type { TrendPoint } from '../types'

interface Props {
  data: TrendPoint[]
  currentHour: number
}

export default function ZoneTrendChart({ data, currentHour }: Props) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
        <XAxis
          dataKey="hour"
          tick={{ fill: 'rgba(255,255,255,0.28)', fontSize: 10, fontFamily: 'JetBrains Mono' }}
          tickLine={false}
          axisLine={false}
          interval={3}
          tickFormatter={h => `${h}h`}
        />
        <YAxis
          tick={{ fill: 'rgba(255,255,255,0.28)', fontSize: 10 }}
          tickLine={false}
          axisLine={false}
          width={32}
        />
        <Tooltip
          contentStyle={{
            background: '#111113',
            border: '1px solid rgba(255,255,255,0.06)',
            borderRadius: 8,
            fontSize: 12,
            color: '#fff',
          }}
          formatter={(v: number) => [v, 'Avg trips']}
          labelFormatter={h => `${h}:00`}
          cursor={{ stroke: 'rgba(255,255,255,0.08)' }}
        />
        <ReferenceLine
          x={currentHour}
          stroke="rgba(245,158,11,0.7)"
          strokeWidth={1.5}
          strokeDasharray="3 3"
        />
        <Line
          type="monotone"
          dataKey="trips"
          stroke="#f59e0b"
          strokeWidth={1.5}
          dot={false}
          activeDot={{ r: 3, fill: '#fbbf24', stroke: 'none' }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
