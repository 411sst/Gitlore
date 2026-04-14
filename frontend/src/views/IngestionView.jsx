import { useEffect, useRef, useState } from 'react'
import API from '../config/api'

const STAGE_DEFS = [
  { key: 'cloning',        label: 'Cloning',                  defaultMsg: 'Waiting to start...' },
  { key: 'parsing_code',   label: 'Parsing code',             defaultMsg: 'Waiting for clone to complete' },
  { key: 'parsing_commits',label: 'Parsing commits',          defaultMsg: 'Waiting for code parse to complete' },
  { key: 'fetching_prs',   label: 'Fetching PRs',             defaultMsg: 'Waiting for commit parse to complete' },
  { key: 'building_graph', label: 'Building knowledge graph', defaultMsg: 'Pending preceding stage' },
  { key: 'embedding',      label: 'Embedding',                defaultMsg: 'Vectorizing semantic units' },
  { key: 'building_bm25',  label: 'Building search index',    defaultMsg: 'Optimizing for low-latency retrieval' },
  { key: 'complete',       label: 'Complete',                 defaultMsg: 'Finalizing ingestion pipeline' },
]

const STAGE_KEYS = STAGE_DEFS.map(s => s.key)

function stageIndex(key) {
  return STAGE_KEYS.indexOf(key)
}

export default function IngestionView({ repoConfig, onComplete, onBack }) {
  const [stageMessages, setStageMessages] = useState({})
  const [activeStage, setActiveStage] = useState(null)   // key of current stage
  const [errorMsg, setErrorMsg] = useState(null)
  const [percent, setPercent] = useState(0)
  const [logs, setLogs] = useState([])
  const [done, setDone] = useState(false)
  const logEndRef = useRef(null)
  const esRef = useRef(null)
  const doneRef = useRef(false)

  // Derive repo display name from URL
  const repoDisplay = repoConfig?.repoUrl
    ? repoConfig.repoUrl.replace(/^https?:\/\/github\.com\//, '').replace(/\.git$/, '')
    : ''
  const dateRange = [repoConfig?.startDate, repoConfig?.endDate].filter(Boolean).join(' \u2192 ')

  useEffect(() => {
    const es = new EventSource(`${API}/ingest/progress`)
    esRef.current = es

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        const { stage, message, percent: pct } = data

        if (stage === 'error') {
          setErrorMsg(message || 'An unknown error occurred.')
          es.close()
          return
        }

        // Update percent
        if (typeof pct === 'number') setPercent(pct)

        // Update stage messages
        if (stage) {
          setActiveStage(stage)
          setStageMessages(prev => ({ ...prev, [stage]: message }))
        }

        // Append log line
        const ts = new Date().toISOString().replace('T', ' ').slice(0, 23)
        setLogs(prev => [...prev, { ts, text: message, stage }])

        if (stage === 'complete') {
          doneRef.current = true
          setDone(true)
          es.close()
        }
      } catch (_) { /* ignore malformed events */ }
    }

    es.onerror = () => {
      if (doneRef.current || es.readyState === EventSource.CLOSED) {
        return
      }
      setErrorMsg(prev => prev || 'Connection to server lost.')
      es.close()
    }

    return () => es.close()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-scroll logs
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  function getDotClass(stageKey) {
    if (errorMsg && activeStage === stageKey) {
      return 'w-[10px] h-[10px] rounded-full bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.5)]'
    }
    const activeIdx = stageIndex(activeStage)
    const thisIdx = stageIndex(stageKey)
    if (activeIdx === -1) {
      return 'w-[10px] h-[10px] rounded-full bg-surface-variant/50'
    }
    if (thisIdx < activeIdx) {
      return 'w-[10px] h-[10px] rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.4)]'
    }
    if (thisIdx === activeIdx) {
      if (stageKey === 'complete') {
        return 'w-[10px] h-[10px] rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.4)]'
      }
      return 'w-[10px] h-[10px] rounded-full bg-primary-container pulse-amber shadow-[0_0_12px_rgba(245,166,35,0.3)]'
    }
    return 'w-[10px] h-[10px] rounded-full bg-surface-variant/50'
  }

  function getRowClass(stageKey) {
    const activeIdx = stageIndex(activeStage)
    const thisIdx = stageIndex(stageKey)
    if (activeIdx === -1 || thisIdx > activeIdx) return 'opacity-40'
    return ''
  }

  function getMessageForStage(stageKey) {
    if (stageMessages[stageKey]) return stageMessages[stageKey]
    const def = STAGE_DEFS.find(s => s.key === stageKey)
    return def?.defaultMsg ?? ''
  }

  const activeStageLabel = STAGE_DEFS.find(s => s.key === activeStage)?.label ?? ''
  const activeStageNum = activeStage ? stageIndex(activeStage) + 1 : 0

  return (
    <div className="flex flex-col h-screen px-10 pt-10 pb-8 overflow-hidden bg-surface text-on-surface font-body selection:bg-primary-container selection:text-on-primary antialiased">

      {/* Header */}
      <header className="mb-12">
        <div className="flex items-baseline gap-4">
          <h1 className="text-3xl font-extrabold tracking-tighter text-primary-container font-headline">Gitlore</h1>
          <div className="h-4 w-[1px] bg-outline-variant/30" />
          <div className="flex flex-col">
            <span className="text-sm font-mono text-on-surface-variant font-medium tracking-tight">
              {repoDisplay || 'repository'}
            </span>
            {dateRange && (
              <span className="text-[10px] font-mono text-on-surface-variant/50 uppercase tracking-widest">
                {dateRange}
              </span>
            )}
          </div>
        </div>
      </header>

      {/* Main grid */}
      <main className="flex-grow grid grid-cols-2 gap-6 min-h-0">

        {/* Stage list */}
        <section className="flex flex-col">
          <div className="relative pl-8 h-full overflow-y-auto custom-scrollbar">
            <div className="absolute left-1 top-2 bottom-2 w-[1px] bg-outline-variant/20" />
            <div className="space-y-8 relative">
              {STAGE_DEFS.map(({ key, label }) => (
                <div key={key} className={`relative group ${getRowClass(key)}`}>
                  <div className={`absolute -left-[31px] top-1.5 ${getDotClass(key)}`} />
                  <div className="flex flex-col">
                    <span className="text-sm font-semibold text-on-surface">{label}</span>
                    <span className={`text-xs ${activeStage && stageIndex(key) === stageIndex(activeStage) ? 'text-on-surface-variant' : 'text-on-surface-variant/60'}`}>
                      {getMessageForStage(key)}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Log panel */}
        <section className="flex flex-col bg-[#111111] rounded-lg border border-outline-variant/10 overflow-hidden">
          <div className="px-4 py-2 bg-surface-container-lowest border-b border-outline-variant/10 flex items-center justify-between flex-shrink-0">
            <span className="text-[10px] uppercase tracking-widest font-mono text-on-surface-variant/50">Runtime Logs</span>
            <div className="flex gap-1.5">
              <div className="w-2 h-2 rounded-full bg-red-500/20" />
              <div className="w-2 h-2 rounded-full bg-amber-500/20" />
              <div className="w-2 h-2 rounded-full bg-emerald-500/20" />
            </div>
          </div>
          <div className="flex-grow p-5 font-mono text-[12px] text-[#4ADE80] overflow-y-auto custom-scrollbar leading-relaxed">
            <div className="space-y-1">
              {logs.length === 0 && (
                <p className="opacity-30">Waiting for ingestion to begin...</p>
              )}
              {logs.map((log, i) => (
                <p
                  key={i}
                  className={i === logs.length - 1 ? 'font-bold' : 'opacity-60'}
                >
                  {log.ts} &nbsp;{log.text}
                </p>
              ))}
              {errorMsg && (
                <p className="text-red-400 font-bold">[ERROR] {errorMsg}</p>
              )}
              <div ref={logEndRef} />
            </div>
          </div>
        </section>
      </main>

      {/* Footer */}
      <footer className="mt-auto pt-8">
        <div className="w-full bg-surface-container-lowest h-1 rounded-full overflow-hidden">
          <div
            className="bg-primary-container h-full shadow-[0_0_10px_rgba(245,166,35,0.4)] transition-all duration-500"
            style={{ width: `${percent}%` }}
          />
        </div>
        <div className="mt-4 flex justify-between items-center">
          <div className="flex items-center gap-3">
            {errorMsg ? (
              <>
                <span className="text-xs font-mono text-red-400/60">STATUS: ERROR</span>
                <button
                  onClick={onBack}
                  className="text-xs font-mono text-on-surface-variant/60 hover:text-on-surface border border-outline-variant/30 px-3 py-1 rounded-lg transition-colors"
                >
                  Back to setup
                </button>
              </>
            ) : done ? (
              <>
                <span className="text-xs font-mono text-emerald-400/60">STATUS: COMPLETE</span>
                <button
                  onClick={onComplete}
                  className="text-xs font-semibold bg-primary-container text-[#0D0D0D] px-4 py-1.5 rounded-lg hover:brightness-110 transition-all"
                >
                  Start asking questions
                </button>
              </>
            ) : (
              <>
                <span className="text-xs font-mono text-on-surface-variant/40">STATUS: PROCESSING</span>
                {activeStage && (
                  <span className="text-xs text-on-surface-variant">
                    Stage {activeStageNum} of {STAGE_DEFS.length} ·{' '}
                    <span className="text-on-surface font-medium">{activeStageLabel}</span>
                  </span>
                )}
              </>
            )}
          </div>
          <div className="text-[10px] font-mono text-on-surface-variant/30 uppercase tracking-[0.2em]">
            {percent > 0 && !done && !errorMsg ? `${percent}% complete` : ''}
          </div>
        </div>
      </footer>

      {/* Subtle ambient gradient overlay */}
      <div className="fixed inset-0 pointer-events-none z-50 overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-tr from-primary-container/[0.02] via-transparent to-transparent opacity-20" />
      </div>
    </div>
  )
}
