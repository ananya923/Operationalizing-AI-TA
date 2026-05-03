import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

interface Props {
  data: Record<string, number>
}

const COLORS: Record<string, string> = {
  Manhattan: '#3b82f6',
  Brooklyn:  '#f59e0b',
  Queens:    '#10b981',
  Bronx:     '#8b5cf6',
  Staten:    '#6b7280',
  EWR:       '#6b7280',
}

export default function BoroughChart({ data }: Props) {
  const chartData = Object.entries(data)
    .map(([name, pct]) => ({ name: name.replace(' Island', ''), pct }))
    .sort((a, b) => b.pct - a.pct)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={chartData} layout="vertical" margin={{ top: 0, right: 8, bottom: 0, left: 0 }}>
        <XAxis
          type="number"
          tick={{ fill: 'rgba(255,255,255,0.28)', fontSize: 10 }}
          tickLine={false}
          axisLine={false}
          tickFormatter={v => `${v}%`}
        />
        <YAxis
          type="category"
          dataKey="name"
          tick={{ fill: 'rgba(255,255,255,0.52)', fontSize: 11 }}
          tickLine={false}
          axisLine={false}
          width={68}
        />
        <Tooltip
          contentStyle={{
            background: '#111113',
            border: '1px solid rgba(255,255,255,0.06)',
            borderRadius: 8,
            fontSize: 12,
            color: '#fff',
          }}
          formatter={(v: number) => [`${v}%`, 'Share']}
          cursor={{ fill: 'rgba(255,255,255,0.04)' }}
        />
        <Bar dataKey="pct" radius={[0, 3, 3, 0]}>
          {chartData.map((entry) => (
            <Cell key={entry.name} fill={COLORS[entry.name] ?? '#4b5563'} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}
