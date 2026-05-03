import { motion } from 'framer-motion'
import type { RankingRow } from '../types'
import './RankingTable.css'

interface Props {
  rows: RankingRow[]
  selectedZone: number | null
  onSelect: (zoneId: number) => void
}

const TREND_ICON = { up: '↑', down: '↓', flat: '—' }
const TREND_CLASS = { up: 'trend-up', down: 'trend-down', flat: 'trend-flat' }

export default function RankingTable({ rows, selectedZone, onSelect }: Props) {
  return (
    <div className="ranking-table">
      <div className="ranking-head">
        <span>#</span>
        <span>Zone</span>
        <span>Trips</span>
        <span>Share</span>
        <span></span>
      </div>
      <div className="ranking-body">
        {rows.map((row, i) => (
          <motion.div
            key={row.zone_id}
            className={`ranking-row ${selectedZone === row.zone_id ? 'selected' : ''}`}
            onClick={() => onSelect(row.zone_id)}
            initial={{ opacity: 0, x: -6 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: i * 0.02, duration: 0.18 }}
          >
            <span className="rank-num">{row.rank}</span>
            <div className="zone-cell">
              <span className="zone-name">{row.name}</span>
              <span className="zone-borough">{row.borough}{row.is_airport ? ' · Airport' : ''}</span>
            </div>
            <span className="trips-val">{row.trips.toFixed(0)}</span>
            <div className="bar-cell">
              <div className="bar-bg">
                <motion.div
                  className="bar-fill"
                  initial={{ width: 0 }}
                  animate={{ width: `${row.pct_of_max}%` }}
                  transition={{ delay: i * 0.02 + 0.1, duration: 0.35 }}
                />
              </div>
              <span className="bar-pct">{row.pct_of_max}%</span>
            </div>
            <span className={`trend-icon ${TREND_CLASS[row.trend]}`}>
              {TREND_ICON[row.trend]}
            </span>
          </motion.div>
        ))}
      </div>
    </div>
  )
}
