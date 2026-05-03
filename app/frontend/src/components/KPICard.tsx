import { motion } from 'framer-motion'
import type { ReactNode } from 'react'
import './KPICard.css'

interface Props {
  label: string
  value: string | number
  sub?: string
  trend?: number
  icon?: ReactNode
}

export default function KPICard({ label, value, sub, trend }: Props) {
  return (
    <motion.div
      className="kpi-card"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.2 }}
    >
      <div className="kpi-header">
        <span className="kpi-label">{label}</span>
      </div>
      <div className="kpi-value">{value}</div>
      {(sub || trend !== undefined) && (
        <div className="kpi-sub">
          {trend !== undefined && (
            <span className={`kpi-trend ${trend >= 0 ? 'up' : 'down'}`}>
              {trend >= 0 ? '+' : ''}{trend}%
            </span>
          )}
          {sub && <span>{sub}</span>}
        </div>
      )}
    </motion.div>
  )
}
