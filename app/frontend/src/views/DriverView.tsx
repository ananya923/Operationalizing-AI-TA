import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api'
import type { useTime } from '../hooks/useTime'
import type { Recommendation, ZoneInfo } from '../types'
import DemandMap from '../components/DemandMap'
import './DriverView.css'

interface Props {
  timeState: ReturnType<typeof useTime>
}

const HOLIDAYS: { [key: string]: string } = {
  '1-1': 'New Year\'s Day', '1-19': 'MLK Day', '2-14': 'Valentine\'s Day',
  '3-17': 'St. Patrick\'s Day', '5-27': 'Memorial Day', '7-4': 'Independence Day',
  '9-2': 'Labor Day', '10-31': 'Halloween', '11-28': 'Thanksgiving', '11-29': 'Thanksgiving+1',
  '12-24': 'Christmas Eve', '12-25': 'Christmas', '12-26': 'Boxing Day', '12-31': 'New Year\'s Eve',
}

const getHolidayForDate = (date: Date): string => {
  const month = date.getMonth() + 1
  const day = date.getDate()
  return HOLIDAYS[`${month}-${day}`] || 'regular'
}

export default function DriverView({ timeState }: Props) {
  const { hour, dow, dateStr } = timeState
  const [currentZone, setCurrentZone] = useState<number>(161)
  const [sheetExpanded, setSheetExpanded] = useState(false)

  const holiday = getHolidayForDate(new Date(dateStr))

  const { data: zone } = useQuery({
    queryKey: ['zone', currentZone, hour, dow, dateStr, holiday],
    queryFn: () => api.zoneInfo(currentZone, hour, dow, dateStr, holiday),
  })

  const { data: recs } = useQuery({
    queryKey: ['recs', currentZone, hour, dow, dateStr, holiday],
    queryFn: () => api.recommendations(currentZone, hour, dow, dateStr, holiday),
  })

  const { data: heatmap } = useQuery({
    queryKey: ['heatmap', hour, dow, dateStr, holiday],
    queryFn: () => api.heatmap(hour, dow, dateStr, holiday),
  })

  const topRec = recs?.[0]
  const demandColor = (level?: string) => {
    switch(level) {
      case 'High': return 'var(--red)'
      case 'Medium': return 'var(--amber)'
      default: return 'var(--text-2)'
    }
  }

  return (
    <div className="driver-mobile">
      {/* Full-screen map */}
      <div className="dm-map">
        <DemandMap
          heatmap={heatmap}
          selectedZone={currentZone}
          onZoneClick={setCurrentZone}
          compact
        />

        {/* Time badge */}
        <div className="dm-time-badge">
          <div className="dm-time">{timeState.hourLabel}</div>
          <div className="dm-dow">{timeState.dowLabel}</div>
        </div>

        {/* Current location pill */}
        {zone && (
          <div className="dm-location-pill">
            <span className="dm-loc-dot" />
            <span>{zone.name}</span>
          </div>
        )}
      </div>

      {/* Bottom sheet */}
      <div className={`dm-sheet ${sheetExpanded ? 'expanded' : ''}`}>
        <div className="dm-handle" onClick={() => setSheetExpanded(!sheetExpanded)} />

        {holiday !== 'regular' && (
          <div className="dm-context">{holiday}</div>
        )}

        {/* Top recommendation */}
        {topRec && zone && (
          <div className="dm-rec-primary">
            <div className="dm-rec-zone">{topRec.name}</div>
            <div className="dm-rec-meta">
              <span>{topRec.drive_minutes} min away</span>
              <span className="dm-dot">·</span>
              <span style={{ color: demandColor(zone.demand_level) }}>
                {zone.demand_level} demand
              </span>
            </div>
            <div className="dm-earnings">
              <span className="dm-earn-value">${topRec.est_yield_min}-${topRec.est_yield_max}</span>
              <span className="dm-earn-label">/hr estimated</span>
            </div>
            <button
              className="dm-cta"
              onClick={() => setCurrentZone(topRec.zone_id)}
            >
              Head There
            </button>
          </div>
        )}

        {/* Alternative zones */}
        {recs && recs.length > 1 && (
          <>
            <div className="dm-alts-label">Other zones</div>
            <div className="dm-alts">
              {recs.slice(1).map(rec => (
                <div
                  key={rec.zone_id}
                  className="dm-alt-card"
                  onClick={() => setCurrentZone(rec.zone_id)}
                >
                  <div className="dm-alt-name">{rec.name}</div>
                  <div className="dm-alt-drive">{rec.drive_minutes} min</div>
                  <div className="dm-alt-earn">${rec.est_yield_max}/hr</div>
                </div>
              ))}
            </div>
          </>
        )}

        {/* Footer hint */}
        <div className="dm-hint">Tap zone on map or card to navigate</div>
      </div>
    </div>
  )
}
