export interface HeatmapData {
  demand: Record<string, number>
  max: number
  hour: number
  dow: number
}

export interface KPIData {
  total_trips: number
  active_zones: number
  peak_zone_id: number
  peak_zone_name: string
  peak_zone_trips: number
  hour_trend: number[]
  borough_breakdown: Record<string, number>
  vs_prev_hour_pct: number
}

export interface RankingRow {
  rank: number
  zone_id: number
  name: string
  borough: string
  trips: number
  pct_of_max: number
  trend: 'up' | 'down' | 'flat'
  is_airport: boolean
}

export interface TrendPoint {
  hour: number
  trips: number
}

export interface Recommendation {
  rank: number
  zone_id: number
  name: string
  borough: string
  trips: number
  demand_score: number
  drive_minutes: number
  est_yield_min: number
  est_yield_max: number
}

export interface ZoneInfo {
  zone_id: number
  name: string
  borough: string
  trips: number
  demand_pct: number
  demand_level: 'High' | 'Medium' | 'Low'
}

export interface ZoneMeta {
  zone_id: number
  name: string
  borough: string
  service_zone: string
}

export interface ForecastPoint {
  time_bucket: string
  predicted_trips: number
  hour: number
  minute: number
}

export interface OperatorZone {
  zone_id: number
  name: string
  borough: string
  demand_now: number
  demand_next_hour: number
  change_pct: number
  unmet_demand: number
  drivers_needed: number
  revenue_potential: number
  supply_status: 'tight' | 'balanced' | 'light'
  is_forecast: boolean
  forecast_source: 'actual' | 'lgbm'
  volatility_label: 'low' | 'medium' | 'high'
  volatility_score: number
  trend_direction: 'rising' | 'stable' | 'falling'
  action: string | null
}

export type ViewMode = 'operator' | 'driver'
