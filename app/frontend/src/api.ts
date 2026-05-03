import type {
  HeatmapData, KPIData, RankingRow, TrendPoint,
  Recommendation, ZoneInfo, ZoneMeta, ForecastPoint, OperatorZone
} from './types'

const BASE = '/api'

async function get<T>(path: string, params: Record<string, string | number> = {}): Promise<T> {
  const qs = new URLSearchParams(
    Object.entries(params).map(([k, v]) => [k, String(v)])
  ).toString()
  const res = await fetch(`${BASE}${path}${qs ? `?${qs}` : ''}`)
  if (!res.ok) throw new Error(`API error ${res.status}`)
  return res.json()
}

export const api = {
  heatmap: (hour: number, dow: number, date: string, holiday = "regular") =>
    get<HeatmapData>('/heatmap', { hour, dow, date, holiday }),

  syntheticDemand: (hour: number, dow: number, date: string) =>
    get<any>('/synthetic-demand', { hour, dow, date }),

  forecastHeatmap: (hoursAhead: number) =>
    get<HeatmapData>('/heatmap-forecast', { hours_ahead: hoursAhead }),

  kpis: (hour: number, dow: number, date: string, holiday = "regular") =>
    get<KPIData>('/kpis', { hour, dow, date, holiday }),

  ranking: (hour: number, dow: number, date: string, n = 15, holiday = "regular") =>
    get<RankingRow[]>('/ranking', { hour, dow, date, n, holiday }),

  zoneTrend: (zoneId: number, dow: number, date: string, holiday = "regular") =>
    get<TrendPoint[]>(`/zone/${zoneId}/trend`, { dow, date, holiday }),

  zoneInfo: (zoneId: number, hour: number, dow: number, date: string, holiday = "regular") =>
    get<ZoneInfo>(`/zone/${zoneId}`, { hour, dow, date, holiday }),

  recommendations: (zoneId: number, hour: number, dow: number, date: string, holiday = "regular") =>
    get<Recommendation[]>('/recommendations', { zone_id: zoneId, hour, dow, date, holiday }),

  holidays: () =>
    get<{ holidays: Array<{ date: string; name: string }> }>('/holidays'),

  zonesMeta: () =>
    get<ZoneMeta[]>('/zones/metadata'),

  forecast: (zoneId: number, hour: number, dow: number, date: string, steps = 16) =>
    get<ForecastPoint[]>('/forecast', { zone_id: zoneId, hour, dow, date, steps }),

  operatorZones: (hour: number, dow: number, date: string, holiday = 'regular') =>
    get<OperatorZone[]>('/operator/zones', { hour, dow, date, holiday }),
}
