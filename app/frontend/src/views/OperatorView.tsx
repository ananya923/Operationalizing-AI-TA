import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { useTime } from '../hooks/useTime'
import type { OperatorZone } from '../types'
import DemandMap from '../components/DemandMap'
import './OperatorView.css'

interface Props {
  timeState: ReturnType<typeof useTime>
}

const HOLIDAYS: { [key: string]: string } = {
  '1-1': 'New Year\'s Day',
  '1-19': 'MLK Day',
  '2-14': 'Valentine\'s Day',
  '3-17': 'St. Patrick\'s Day',
  '5-27': 'Memorial Day',
  '7-4': 'Independence Day',
  '9-2': 'Labor Day',
  '10-31': 'Halloween',
  '11-28': 'Thanksgiving',
  '11-29': 'Thanksgiving+1',
  '12-24': 'Christmas Eve',
  '12-25': 'Christmas',
  '12-26': 'Boxing Day',
  '12-31': 'New Year\'s Eve',
}

const getHolidayForDate = (date: Date): string => {
  const month = date.getMonth() + 1
  const day = date.getDate()
  const key = `${month}-${day}`
  return HOLIDAYS[key] || 'regular'
}

export default function OperatorView({ timeState }: Props) {
  const { hour, dow, dateStr } = timeState
  const [selectedZone, setSelectedZone] = useState<number | null>(null)
  const [surgePrices, setSurgePrices] = useState<Record<number, number>>({})
  const [globalSurge, setGlobalSurge] = useState(1.0)

  const REFERENCE_DATE = '2026-02-14'
  const isFutureDate = dateStr > REFERENCE_DATE
  const holiday = getHolidayForDate(new Date(dateStr))

  const { data: heatmap } = useQuery({
    queryKey: ['heatmap', hour, dow, dateStr, holiday],
    queryFn: () => api.heatmap(hour, dow, dateStr, holiday),
  })

  const { data: operatorZones } = useQuery({
    queryKey: ['operatorZones', hour, dow, dateStr, holiday],
    queryFn: () => api.operatorZones(hour, dow, dateStr, holiday),
  })

  const totalDemand = operatorZones?.reduce((sum: number, z: OperatorZone) => sum + z.demand_now, 0) || 0
  const totalUnmet = operatorZones?.reduce((sum: number, z: OperatorZone) => sum + z.unmet_demand, 0) || 0
  const tightZones = operatorZones?.filter((z: OperatorZone) => z.supply_status === 'tight').length || 0

  const handleSurgeChange = (zoneId: number, value: number) => {
    setSurgePrices(prev => ({ ...prev, [zoneId]: value }))
  }

  const calculateFleetRevenue = () => {
    return operatorZones?.reduce((sum: number, z: OperatorZone) =>
      sum + (z.revenue_potential * (surgePrices[z.zone_id] || 1.0)), 0) || 0
  }

  const tightZonesList = operatorZones?.filter((z: OperatorZone) => z.supply_status === 'tight') || []

  const handleQuickAction = (action: string) => {
    if (action === 'surge-tight-1.5x') {
      const newSurges = { ...surgePrices }
      tightZonesList.forEach((z: OperatorZone) => { newSurges[z.zone_id] = 1.5 })
      setSurgePrices(newSurges)
    } else if (action === 'reset-all') {
      setSurgePrices({})
    }
  }

  const topZones = operatorZones?.slice(0, 12) || []
  const fleetRevenue = calculateFleetRevenue()

  return (
    <div className="op-layout">
      {/* Left — Metrics & Status */}
      <aside className="op-left">
        <div className="metric-section">
          <div style={{
            fontSize: '12px',
            fontWeight: 500,
            color: isFutureDate ? 'var(--amber)' : 'var(--green)',
            textTransform: 'uppercase',
            letterSpacing: '0.05em'
          }}>
            {isFutureDate ? 'Forecast' : 'Actual'}
          </div>
          {holiday !== 'regular' && (
            <div style={{ fontSize: '10px', color: 'var(--text-2)', marginTop: '6px' }}>
              {holiday}
            </div>
          )}
        </div>

        <div className="metric-card">
          <div className="metric-label">Total Demand</div>
          <div className="metric-value">{Math.round(totalDemand)}</div>
          <div className="metric-sub">trips/hour now</div>
        </div>

        <div className="metric-card">
          <div className="metric-label">Unmet Demand</div>
          <div className="metric-value" style={{ color: totalUnmet > 0 ? 'var(--red)' : 'var(--green)' }}>
            {totalUnmet}
          </div>
          <div className="metric-sub">{tightZones} zones tight</div>
        </div>

        <div className="metric-card">
          <div className="metric-label">Supply Positioning</div>
          <div style={{ fontSize: '11px', color: 'var(--text-2)', marginTop: '8px', lineHeight: 1.6 }}>
            {tightZones > 0 ? (
              <>
                <span style={{ color: 'var(--amber)', fontWeight: 500 }}>{tightZones} zones</span> need drivers
                <br />
                <span style={{ fontSize: '10px' }}>Adjust surge or bonus</span>
              </>
            ) : (
              'Balanced supply'
            )}
          </div>
        </div>

        <div className="fleet-revenue-card">
          <div className="fleet-revenue-label">Fleet Revenue</div>
          <div className="fleet-revenue-value">${Math.round(fleetRevenue).toLocaleString()}</div>
          <div className="fleet-revenue-sub">with current surge</div>
        </div>

        <div style={{ padding: '0 16px', display: 'flex', flexDirection: 'column', gap: '8px' }}>
          <button className="quick-action-btn" onClick={() => handleQuickAction('surge-tight-1.5x')}>
            Surge tight zones 1.5x
          </button>
          <button className="quick-action-btn" onClick={() => handleQuickAction('reset-all')}>
            Reset all surge
          </button>
        </div>

        <div style={{ fontSize: '10px', color: 'var(--text-3)', padding: '12px 0', marginTop: 'auto', borderTop: '1px solid var(--border)', paddingTop: '12px' }}>
          Operator controls on right
        </div>
      </aside>

      {/* Center — Map */}
      <div className="op-map">
        <DemandMap
          heatmap={heatmap}
          selectedZone={selectedZone}
          onZoneClick={setSelectedZone}
        />
      </div>

      {/* Right — Zone Opportunities */}
      <aside className="op-right">
        <div style={{ padding: '14px 16px', borderBottom: '1px solid var(--border)', fontSize: '11px', fontWeight: 600, textTransform: 'uppercase', color: 'var(--text-2)', letterSpacing: '0.05em' }}>
          Top Zones by Opportunity
        </div>

        <div className="zones-list">
          {topZones.map((zone: OperatorZone, i: number) => (
            <motion.div
              key={zone.zone_id}
              className={`zone-row ${selectedZone === zone.zone_id ? 'selected' : ''}`}
              initial={{ opacity: 0, y: 4 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.03 }}
              onClick={() => setSelectedZone(zone.zone_id)}
            >
              <div className="zone-row-header">
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flex: 1 }}>
                  <div className="zone-name">{zone.name}</div>
                  {zone.is_forecast && <span className="forecast-badge">LGB</span>}
                  <span className={`volatility-badge volatility-${zone.volatility_label}`}>
                    {zone.volatility_label}
                  </span>
                </div>
                <div className="zone-demand">{Math.round(zone.demand_now)} trips/hr</div>
              </div>

              <div className="zone-metrics">
                <div className="metric-pair">
                  <span className="label">Forecast</span>
                  <span className={`value ${zone.change_pct > 0 ? 'up' : zone.change_pct < 0 ? 'down' : 'flat'}`}>
                    {zone.change_pct > 0 ? '+' : ''}{zone.change_pct}%
                    <span className={`trend-icon trend-${zone.trend_direction}`}>
                      {zone.trend_direction === 'rising' ? ' ↑' : zone.trend_direction === 'falling' ? ' ↓' : ' →'}
                    </span>
                  </span>
                </div>
                <div className="metric-pair">
                  <span className="label">Unmet</span>
                  <span className={`value ${zone.unmet_demand > 0 ? 'warn' : ''}`}>
                    {zone.unmet_demand}
                  </span>
                </div>
                <div className="metric-pair">
                  <span className="label">Status</span>
                  <span className={`status ${zone.supply_status}`}>
                    {zone.supply_status}
                  </span>
                </div>
              </div>

              {zone.action && <div className="zone-action">{zone.action}</div>}

              <div className="surge-control">
                <label className="surge-label">Surge: {(surgePrices[zone.zone_id] || 1.0).toFixed(1)}x</label>
                <input
                  type="range"
                  min="1.0"
                  max="3.0"
                  step="0.1"
                  value={surgePrices[zone.zone_id] || 1.0}
                  onChange={e => handleSurgeChange(zone.zone_id, parseFloat(e.target.value))}
                  className="surge-slider"
                />
                <div className="surge-stats">
                  <span className="surge-stat">Est. rev: ${Math.round(zone.revenue_potential * (surgePrices[zone.zone_id] || 1.0))}</span>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </aside>
    </div>
  )
}
