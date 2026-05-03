import { useState, useCallback } from 'react'

const DOW_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

// Fixed reference point: end of 2nd week in Feb 2026 (latest complete month in dataset)
// This is treated as "now" for the app, even though the actual date is later
const REFERENCE_DATE = new Date('2026-02-14T12:00:00Z')

const getWeekRange = (date: Date) => {
  const d = new Date(date)
  const dow = d.getDay()
  // Move to Monday of this week
  const diff = d.getDate() - dow + (dow === 0 ? -6 : 1)
  const monday = new Date(d.setDate(diff))

  const sunday = new Date(monday)
  sunday.setDate(monday.getDate() + 6)

  return { monday, sunday }
}

const formatDate = (date: Date) => {
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

export function useTime() {
  const [date, setDate] = useState(new Date(REFERENCE_DATE))
  const [hour, setHour] = useState(REFERENCE_DATE.getHours())
  const [live, setLive] = useState(false)

  const jsDow = date.getDay()
  const dow = jsDow === 0 ? 6 : jsDow - 1
  const dateStr = date.toISOString().split('T')[0]
  
  const weekRange = getWeekRange(date)
  const weekLabel = `Week of ${formatDate(weekRange.monday)} – ${formatDate(weekRange.sunday)}`

  const syncToNow = useCallback(() => {
    const n = new Date()
    setDate(new Date(n))
    setHour(n.getHours())
  }, [])

  const toggleLive = useCallback(() => {
    setLive(prev => {
      if (!prev) syncToNow()
      return !prev
    })
  }, [syncToNow])

  const setDow = useCallback((dayIndex: number) => {
    // dayIndex is 0=Mon, 1=Tue, ..., 6=Sun
    // Get Monday of current week
    const d = new Date(date)
    const currentDow = d.getDay()
    const diff = d.getDate() - currentDow + (currentDow === 0 ? -6 : 1)
    const monday = new Date(d.setDate(diff))
    
    // Add days to get to the selected day
    const targetDate = new Date(monday)
    targetDate.setDate(monday.getDate() + dayIndex)
    setDate(targetDate)
  }, [date])

  const addDays = useCallback((days: number) => {
    setDate(prev => {
      const next = new Date(prev)
      next.setDate(next.getDate() + days)
      return next
    })
  }, [])

  return {
    hour, setHour,
    date, setDate, dateStr,
    dow, setDow,
    live, toggleLive, syncToNow,
    dowLabel: DOW_LABELS[dow],
    hourLabel: `${String(hour).padStart(2, '0')}:00`,
    weekLabel,
    weekRange,
    addDays,
    DOW_LABELS,
  }
}
