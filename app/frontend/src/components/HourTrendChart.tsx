import {
  AreaChart, Area, XAxis, YAxis, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from 'recharts'

interface Props {
  data: number[]
  currentHour: number
}

export default function HourTrendChart({ data, currentHour }: Props) {
  const chartData = data.map((v, h) => ({ hour: h, trips: v }))

  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
        <defs>
          <linearGradient id="blueGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor="#3b82f6" stopOpacity={0.4} />
            <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <XAxis
          dataKey="hour"
          tick={{ fill: 'rgba(255,255,255,0.22)', fontSize: 10, fontFamily: 'JetBrains Mono' }}
          tickLine={false}
          axisLine={false}
          interval={3}
          tickFormatter={h => `${h}h`}
        />
        <YAxis
          tick={{ fill: 'rgba(255,255,255,0.28)', fontSize: 10 }}
          tickLine={false}
          axisLine={false}
          width={36}
        />
        <Tooltip
          contentStyle={{
            background: '#111113',
            border: '1px solid rgba(255,255,255,0.06)',
            borderRadius: 8,
            fontSize: 12,
            color: '#fff',
          }}
          formatter={(v: number) => [v.toLocaleString(), 'Trips']}
          labelFormatter={h => `${h}:00`}
          cursor={{ stroke: 'rgba(255,255,255,0.1)', strokeWidth: 1 }}
        />
        <ReferenceLine
          x={currentHour}
          stroke="rgba(59,130,246,0.6)"
          strokeWidth={1.5}
          strokeDasharray="3 3"
        />
        <Area
          type="monotone"
          dataKey="trips"
          stroke="#3b82f6"
          strokeWidth={1.5}
          fill="url(#blueGrad)"
          dot={false}
          activeDot={{ r: 3, fill: '#60a5fa', stroke: 'none' }}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}
