import { useRef, useEffect, useCallback } from 'react'
import mapboxgl from 'mapbox-gl'
import type { HeatmapData } from '../types'

mapboxgl.accessToken = import.meta.env.VITE_MAPBOX_TOKEN ?? ''

interface Props {
  heatmap: HeatmapData | undefined
  selectedZone: number | null
  onZoneClick: (zoneId: number) => void
  compact?: boolean
}

const ZONE_FILL   = 'taxi-zones-fill'
const ZONE_LINE   = 'taxi-zones-line'
const ZONE_HOVER  = 'taxi-zones-hover'
const SOURCE_ID   = 'taxi-zones'
const GEOJSON_URL = '/taxi_zones.geojson'

function demandColor(norm: number): string {
  // Deep blue → electric blue → amber → red
  if (norm < 0.25) {
    const t = norm / 0.25
    return `rgba(${Math.round(11 + t * 48)}, ${Math.round(20 + t * 110)}, ${Math.round(40 + t * 186)}, 0.75)`
  } else if (norm < 0.6) {
    const t = (norm - 0.25) / 0.35
    return `rgba(${Math.round(59 + t * 186)}, ${Math.round(130 - t * 100)}, ${Math.round(246 - t * 235)}, 0.80)`
  } else {
    const t = (norm - 0.6) / 0.4
    return `rgba(${Math.round(245 - t * 6)}, ${Math.round(30 + t * 10)}, ${Math.round(11)}, ${0.80 + t * 0.15})`
  }
}

export default function DemandMap({ heatmap, selectedZone, onZoneClick, compact = false }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<mapboxgl.Map | null>(null)
  const hoveredId = useRef<number | null>(null)
  const popupRef = useRef<mapboxgl.Popup | null>(null)

  const updateColors = useCallback((map: mapboxgl.Map, data: HeatmapData) => {
    if (!map.getSource(SOURCE_ID)) return
    const { demand, max } = data
    const colorExpr: mapboxgl.Expression = ['match', ['to-string', ['get', 'LocationID']]]

    let hasEntries = false
    for (const [zid, val] of Object.entries(demand)) {
      const norm = max > 0 ? val / max : 0
      colorExpr.push(zid, demandColor(norm))
      hasEntries = true
    }
    if (!hasEntries) return
    colorExpr.push('rgba(14,21,32,0.6)') // default

    map.setPaintProperty(ZONE_FILL, 'fill-color', colorExpr)
  }, [])

  useEffect(() => {
    if (!containerRef.current) return
    const map = new mapboxgl.Map({
      container: containerRef.current,
      style: 'mapbox://styles/mapbox/dark-v11',
      center: [-73.971, 40.743],
      zoom: compact ? 9.8 : 10.3,
      minZoom: 8,
      maxZoom: 16,
      attributionControl: false,
      logoPosition: 'bottom-right',
    })

    mapRef.current = map

    map.on('load', () => {
      map.addSource(SOURCE_ID, {
        type: 'geojson',
        data: GEOJSON_URL,
        generateId: false,
      })

      map.addLayer({
        id: ZONE_FILL,
        type: 'fill',
        source: SOURCE_ID,
        paint: {
          'fill-color': 'rgba(14,21,32,0.6)',
          'fill-opacity': 1,
        },
      })

      map.addLayer({
        id: ZONE_LINE,
        type: 'line',
        source: SOURCE_ID,
        paint: {
          'line-color': 'rgba(59,130,246,0.25)',
          'line-width': 0.5,
        },
      })

      map.addLayer({
        id: ZONE_HOVER,
        type: 'line',
        source: SOURCE_ID,
        paint: {
          'line-color': 'rgba(96,165,250,0.9)',
          'line-width': 1.5,
        },
        filter: ['==', ['get', 'LocationID'], -1],
      })

      map.on('mousemove', ZONE_FILL, (e) => {
        if (!e.features?.length) return
        const feat = e.features[0]
        const lid = feat.properties?.LocationID as number
        if (hoveredId.current !== lid) {
          hoveredId.current = lid
          map.setFilter(ZONE_HOVER, ['==', ['get', 'LocationID'], lid])
          map.getCanvas().style.cursor = 'pointer'
        }
      })

      map.on('mouseleave', ZONE_FILL, () => {
        hoveredId.current = null
        map.setFilter(ZONE_HOVER, ['==', ['get', 'LocationID'], -1])
        map.getCanvas().style.cursor = ''
        popupRef.current?.remove()
      })

      map.on('click', ZONE_FILL, (e) => {
        if (!e.features?.length) return
        const lid = e.features[0].properties?.LocationID as number
        onZoneClick(lid)
      })
    })

    return () => {
      popupRef.current?.remove()
      map.remove()
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Update colors when heatmap changes
  useEffect(() => {
    const map = mapRef.current
    if (!map || !heatmap) return
    if (map.isStyleLoaded()) {
      updateColors(map, heatmap)
    } else {
      map.once('load', () => updateColors(map, heatmap))
    }
  }, [heatmap, updateColors])

  // Highlight selected zone
  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.isStyleLoaded()) return
    map.setFilter(ZONE_HOVER, [
      '==', ['get', 'LocationID'],
      selectedZone ?? -1,
    ])
  }, [selectedZone])

  return (
    <div
      ref={containerRef}
      style={{ width: '100%', height: '100%', borderRadius: 'inherit' }}
    />
  )
}
