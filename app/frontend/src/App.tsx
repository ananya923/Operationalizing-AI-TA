import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useTime } from './hooks/useTime'
import OperatorView from './views/OperatorView'
import DriverView from './views/DriverView'
import type { ViewMode } from './types'
import './App.css'

export default function App() {
  const [view, setView] = useState<ViewMode>('operator')
  const timeState = useTime()

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="topbar-brand">
          <span className="brand-mark">NYC Cab Analytics</span>
        </div>

        <nav className="view-toggle">
          {(['operator', 'driver'] as ViewMode[]).map(v => (
            <button
              key={v}
              className={`toggle-btn ${view === v ? 'active' : ''}`}
              onClick={() => setView(v)}
            >
              {v === 'operator' ? 'Fleet' : 'Driver'}
            </button>
          ))}
        </nav>

        <div className="topbar-time">
          <TimeControl {...timeState} />
        </div>
      </header>

      <main className="main-content">
        <AnimatePresence mode="wait">
          <motion.div
            key={view}
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            transition={{ duration: 0.18 }}
            style={{ height: '100%' }}
          >
            {view === 'operator'
              ? <OperatorView timeState={timeState} />
              : <DriverView timeState={timeState} />
            }
          </motion.div>
        </AnimatePresence>
      </main>
    </div>
  )
}

function TimeControl({
  hour, setHour, dow, setDow,
  live, toggleLive, hourLabel, DOW_LABELS,
}: ReturnType<typeof useTime>) {
  return (
    <div className="time-control">
      <div className="dow-pills">
        {DOW_LABELS.map((d, i) => (
          <button
            key={d}
            className={`dow-pill ${dow === i ? 'active' : ''}`}
            onClick={() => {
              setDow(i)
              if (live) toggleLive()
            }}
            title={d}
          >
            {d}
          </button>
        ))}
      </div>

      <div className="hour-row">
        <span className="hour-label">{hourLabel}</span>
        <input
          type="range"
          min={0}
          max={23}
          value={hour}
          onChange={e => {
            setHour(Number(e.target.value))
            if (live) toggleLive()
          }}
          className="hour-slider"
        />
        <button
          className={`live-btn ${live ? 'active' : ''}`}
          onClick={toggleLive}
        >
          <span className="live-dot" />
          Live
        </button>
      </div>
    </div>
  )
}
