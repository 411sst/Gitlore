import { useEffect, useRef, useState } from 'react'
import API from '../config/api'

function sourceContextKey(queryId, sourceId) {
  return `${queryId || 'unknown'}::${sourceId}`
}

// Icon per source type
function sourceIcon(type) {
  if (!type) return 'description'
  const t = type.toLowerCase()
  if (t.includes('commit')) return 'commit'
  if (t.includes('pr') || t.includes('pull')) return 'terminal'
  if (t.includes('function') || t.includes('class')) return 'code'
  if (t.includes('file')) return 'description'
  return 'description'
}

// Parse [source_id] citations from answer text
function CitationChips({ citations, queryId, onChipClick, expandedSources }) {
  if (!citations || citations.length === 0) return null
  return (
    <div className="mt-8">
      <div className="flex flex-wrap gap-2 items-center">
        <span className="text-[10px] font-bold text-on-surface/30 uppercase tracking-wider mr-2">Sources:</span>
        {citations.map((cit) => {
          const sourceKey = sourceContextKey(queryId, cit.source_id)
          const isExpanded = !!expandedSources[sourceKey]
          const sourceData = expandedSources[sourceKey]
          const label = cit.source_id || 'unknown'
          return (
            <div key={sourceKey} className="flex flex-col">
              <button
                onClick={() => onChipClick(cit.source_id, queryId)}
                className={`flex items-center gap-2 px-3 py-1 border rounded-full text-[11px] font-mono cursor-pointer transition-colors ${
                  isExpanded
                    ? 'border-[#F5A623]/70 bg-[#F5A623]/10 text-[#F5A623]'
                    : 'border-[#F5A623]/40 text-[#F5A623] hover:bg-[#F5A623]/10'
                }`}
              >
                <span className="material-symbols-outlined text-[14px]">
                  {sourceIcon(cit.type || label)}
                </span>
                {label}
              </button>
              {isExpanded && (
                <div className="mt-2 ml-1 bg-[#0e0e0e] border border-outline-variant/10 rounded-lg p-4 max-w-[560px]">
                  {sourceData === 'loading' ? (
                    <span className="text-[11px] font-mono text-on-surface-variant/50 animate-pulse">Loading source...</span>
                  ) : sourceData?.error ? (
                    <span className="text-[11px] font-mono text-red-400">{sourceData.error}</span>
                  ) : (
                    <pre className="text-[11px] font-mono text-on-surface/70 whitespace-pre-wrap break-words leading-relaxed">
                      {sourceData?.content || JSON.stringify(sourceData, null, 2)}
                    </pre>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function AssistantMessage({ msg, onChipClick, expandedSources }) {
  return (
    <div className="flex flex-col items-start w-full">
      <div className="flex items-center gap-2 mb-3">
        <span className="text-[#F5A623] font-bold text-[10px] tracking-widest uppercase">Gitlore AI</span>
        {msg.isStreaming && (
          <span className="w-1.5 h-1.5 rounded-full bg-[#F5A623] animate-pulse" />
        )}
      </div>
      <div className="bg-[#1E1E1E] px-6 py-6 rounded-xl text-on-surface text-[15px] leading-relaxed border border-outline-variant/5 w-full max-w-[100%]">
        {msg.content
          ? <p style={{ whiteSpace: 'pre-wrap' }}>{msg.content}</p>
          : <span className="text-on-surface/30 animate-pulse">Thinking...</span>
        }
        {!msg.isStreaming && msg.citations && (
          <CitationChips
            citations={msg.citations}
            queryId={msg.queryId}
            onChipClick={onChipClick}
            expandedSources={expandedSources}
          />
        )}
      </div>
    </div>
  )
}

function UserMessage({ msg }) {
  const time = msg.timestamp
    ? new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    : ''
  return (
    <div className="flex flex-col items-end w-full group">
      <div className="bg-surface-container px-5 py-3.5 rounded-xl text-on-surface text-[14px] leading-relaxed max-w-[85%] border border-outline-variant/10">
        {msg.content}
      </div>
      {time && <div className="mt-2 text-[10px] font-mono text-on-surface/20">SENT {time.toUpperCase()}</div>}
    </div>
  )
}

export default function ChatView({ repoConfig, onNewRepo }) {
  const [messages, setMessages] = useState([])
  const [history, setHistory] = useState([])
  const [input, setInput] = useState('')
  const [isStreaming, setIsStreaming] = useState(false)
  const [expandedSources, setExpandedSources] = useState({})  // `${queryId}::${sourceId}` -> data | 'loading'
  const [activeHistoryIdx, setActiveHistoryIdx] = useState(null)
  const messagesEndRef = useRef(null)
  const inputRef = useRef(null)
  const abortRef = useRef(null)

  const repoDisplay = repoConfig?.repoUrl
    ? repoConfig.repoUrl.replace(/^https?:\/\/github\.com\//, '').replace(/\.git$/, '')
    : 'repository'
  const dateRange = [repoConfig?.startDate, repoConfig?.endDate].filter(Boolean).join(' \u2192 ')

  // Load history on mount
  useEffect(() => {
    fetch(`${API}/query/history`)
      .then(r => r.json())
      .then(data => {
        const userMsgs = (data || [])
          .filter(m => m.role === 'user')
          .map(m => ({
            ...m,
            timestamp: m.timestamp || m.created_at || null,
          }))
        setHistory(userMsgs)
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    return () => {
      if (abortRef.current) {
        abortRef.current.abort()
      }
    }
  }, [])

  // Auto-scroll
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // Focus input on mount
  useEffect(() => {
    inputRef.current?.focus()
  }, [])

  async function sendMessage(question) {
    if (!question.trim() || isStreaming) return

    const userMsg = { role: 'user', content: question.trim(), timestamp: new Date().toISOString() }
    const assistantMsg = { role: 'assistant', content: '', isStreaming: true, citations: null, queryId: null }

    setMessages(prev => [...prev, userMsg, assistantMsg])
    setInput('')
    setIsStreaming(true)
    setActiveHistoryIdx(null)

    const controller = new AbortController()
    abortRef.current = controller

    try {
      const res = await fetch(`${API}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: question.trim() }),
        signal: controller.signal,
      })

      if (!res.ok) {
        throw new Error(`Server error ${res.status}`)
      }

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const payload = line.slice(6).trim()
          if (!payload) continue
          try {
            const event = JSON.parse(payload)
            if (event.type === 'token') {
              setMessages(prev => {
                const updated = [...prev]
                const last = { ...updated[updated.length - 1] }
                last.content = (last.content || '') + event.text
                updated[updated.length - 1] = last
                return updated
              })
            } else if (event.type === 'done') {
              setMessages(prev => {
                const updated = [...prev]
                const last = { ...updated[updated.length - 1] }
                last.isStreaming = false
                last.citations = event.citations || []
                last.queryId = event.query_id || null
                updated[updated.length - 1] = last
                return updated
              })
              // Append question to sidebar history
              setHistory(prev => [{ content: question.trim(), timestamp: new Date().toISOString() }, ...prev])
            } else if (event.type === 'error') {
              setMessages(prev => {
                const updated = [...prev]
                const last = { ...updated[updated.length - 1] }
                last.isStreaming = false
                last.content = `[Error] ${event.message}`
                updated[updated.length - 1] = last
                return updated
              })
            }
          } catch (_) { /* malformed JSON */ }
        }
      }
    } catch (err) {
      if (err.name === 'AbortError') {
        setMessages(prev => {
          if (prev.length === 0) return prev
          const updated = [...prev]
          const last = { ...updated[updated.length - 1] }
          last.isStreaming = false
          if (!last.content) {
            last.content = '[Stopped]'
          }
          updated[updated.length - 1] = last
          return updated
        })
      } else {
        setMessages(prev => {
          const updated = [...prev]
          const last = { ...updated[updated.length - 1] }
          last.isStreaming = false
          last.content = `[Error] ${err.message}`
          updated[updated.length - 1] = last
          return updated
        })
      }
    } finally {
      abortRef.current = null
      setIsStreaming(false)
    }
  }

  function stopStreaming() {
    if (!isStreaming || !abortRef.current) return
    abortRef.current.abort()
  }

  async function handleChipClick(sourceId, queryId) {
    const key = sourceContextKey(queryId, sourceId)

    // Toggle
    if (expandedSources[key] && expandedSources[key] !== 'loading') {
      setExpandedSources(prev => {
        const next = { ...prev }
        delete next[key]
        return next
      })
      return
    }

    if (!queryId) {
      setExpandedSources(prev => ({
        ...prev,
        [key]: { error: 'Source context unavailable for this response.' },
      }))
      return
    }

    setExpandedSources(prev => ({ ...prev, [key]: 'loading' }))
    try {
      const res = await fetch(
        `${API}/query/sources/${encodeURIComponent(sourceId)}?query_id=${encodeURIComponent(queryId)}`
      )
      if (!res.ok) throw new Error(`Not found (${res.status})`)
      const data = await res.json()
      setExpandedSources(prev => ({ ...prev, [key]: data }))
    } catch (err) {
      setExpandedSources(prev => ({ ...prev, [key]: { error: err.message } }))
    }
  }

  function handleKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage(input)
    }
  }

  function loadHistoryItem(idx, item) {
    setActiveHistoryIdx(idx)
    // Just highlight — could also re-run the query
  }

  return (
    <div className="flex h-screen overflow-hidden bg-[#0D0D0D]">

      {/* Sidebar */}
      <aside className="w-[260px] h-screen bg-[#161616] border-r border-[#524534]/20 flex flex-col z-40 fixed inset-y-0 left-0">
        {/* Logo */}
        <div className="px-6 h-14 flex items-center border-b border-[#524534]/10">
          <span className="text-[#F5A623] text-lg font-semibold tracking-tight">Gitlore</span>
        </div>

        {/* Repo context */}
        <div className="px-6 py-5">
          <div className="text-[13px] font-medium text-on-surface">{repoDisplay}</div>
          {dateRange && (
            <div className="text-[11px] text-on-surface/40 font-mono mt-1">{dateRange}</div>
          )}
        </div>

        {/* History */}
        <nav className="flex-1 overflow-y-auto px-3 custom-scrollbar">
          <div className="px-3 mb-2 text-[10px] font-bold tracking-[0.1em] text-on-surface/30">HISTORY</div>
          <div className="space-y-1">
            {history.length === 0 && (
              <div className="px-3 py-2 text-[11px] text-on-surface/20 italic">No questions yet</div>
            )}
            {history.map((item, idx) => {
              const isActive = activeHistoryIdx === idx
              return (
                <div
                  key={idx}
                  onClick={() => loadHistoryItem(idx, item)}
                  className={`flex items-center px-3 py-2.5 rounded-lg cursor-pointer transition-colors duration-150 ${
                    isActive
                      ? 'bg-[#201F1F] text-[#F5A623] font-semibold border-l-2 border-[#F5A623]'
                      : 'text-[#E5E2E1]/60 hover:bg-[#201F1F] hover:text-[#E5E2E1]'
                  }`}
                >
                  <span className="text-[12px] truncate">{item.content}</span>
                </div>
              )
            })}
          </div>
        </nav>

        {/* Sidebar footer */}
        <div className="p-4 border-t border-[#524534]/10 space-y-2">
          <button
            onClick={onNewRepo}
            className="w-full py-2 border border-[#524534]/40 text-[#E5E2E1]/60 text-[12px] rounded-lg hover:bg-[#201F1F] transition-all flex items-center justify-center gap-2"
          >
            <span className="material-symbols-outlined text-[16px]">add</span>
            New repository
          </button>
          <div className="flex justify-around py-2">
            <button className="text-[#E5E2E1]/60 hover:text-[#E5E2E1] flex flex-col items-center cursor-pointer transition-colors">
              <span className="material-symbols-outlined text-[18px]">menu_book</span>
            </button>
            <button className="text-[#E5E2E1]/60 hover:text-[#E5E2E1] flex flex-col items-center cursor-pointer transition-colors">
              <span className="material-symbols-outlined text-[18px]">help_outline</span>
            </button>
            <button className="text-[#E5E2E1]/60 hover:text-[#E5E2E1] flex flex-col items-center cursor-pointer transition-colors">
              <span className="material-symbols-outlined text-[18px]">settings</span>
            </button>
          </div>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 ml-[260px] flex flex-col h-screen relative bg-[#0D0D0D]">

        {/* Top bar */}
        <header className="h-14 w-full flex items-center justify-between px-8 border-b border-[#524534]/10 bg-[#0D0D0D]/80 backdrop-blur-xl sticky top-0 z-30">
          <div className="flex items-center gap-2">
            <span className="text-[13px] text-on-surface/50 font-mono tracking-tight">{repoDisplay}</span>
          </div>
          <div className="flex items-center gap-4">
            <span className="material-symbols-outlined text-on-surface/70 hover:text-[#F5A623] cursor-pointer text-[20px] transition-colors">notifications</span>
            <span className="material-symbols-outlined text-on-surface/70 hover:text-[#F5A623] cursor-pointer text-[20px] transition-colors">account_circle</span>
          </div>
        </header>

        {/* Message thread */}
        <div className="flex-1 overflow-y-auto px-6 py-12 custom-scrollbar">
          <div className="max-w-[720px] mx-auto flex flex-col gap-12">
            {messages.length === 0 && (
              <div className="flex flex-col items-center justify-center py-24 text-center">
                <span className="text-[#F5A623] text-4xl font-extrabold tracking-tighter mb-3">Ask anything.</span>
                <p className="text-on-surface/30 text-sm font-mono">
                  Query commits, PRs, functions, and files in{' '}
                  <span className="text-on-surface/50">{repoDisplay}</span>
                </p>
              </div>
            )}
            {messages.map((msg, idx) =>
              msg.role === 'user' ? (
                <UserMessage key={idx} msg={msg} />
              ) : (
                <AssistantMessage
                  key={idx}
                  msg={msg}
                  onChipClick={handleChipClick}
                  expandedSources={expandedSources}
                />
              )
            )}
            <div ref={messagesEndRef} />
          </div>
        </div>

        {/* Input area */}
        <div className="p-8 max-w-[760px] mx-auto w-full sticky bottom-0 bg-gradient-to-t from-[#0D0D0D] via-[#0D0D0D] to-transparent">
          <div className="relative flex items-center bg-surface-container border border-outline-variant/10 rounded-xl overflow-hidden group focus-within:border-[#F5A623]/40 transition-all duration-200">
            <input
              ref={inputRef}
              type="text"
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={`Ask anything about ${repoDisplay}...`}
              disabled={isStreaming}
              className="w-full bg-transparent border-none text-[14px] px-6 py-4 text-on-surface placeholder-on-surface/30 focus:ring-0 focus:outline-none disabled:opacity-50"
            />
            <button
              onClick={() => {
                if (isStreaming) {
                  stopStreaming()
                  return
                }
                sendMessage(input)
              }}
              disabled={!isStreaming && !input.trim()}
              className="mr-3 bg-[#F5A623] text-[#291800] p-2 rounded-lg hover:scale-95 active:scale-90 transition-all flex items-center justify-center disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <span className="material-symbols-outlined font-bold text-[20px]">
                {isStreaming ? 'stop_circle' : 'arrow_forward'}
              </span>
            </button>
          </div>
          <div className="mt-3 flex justify-between items-center px-2">
            <div className="text-[10px] text-on-surface/20 flex items-center gap-4">
              <span className="flex items-center gap-1">
                <span className="material-symbols-outlined text-[12px]">keyboard_return</span>
                to send
              </span>
              <span className="flex items-center gap-1">
                <span className="material-symbols-outlined text-[12px]">history</span>
                history in sidebar
              </span>
            </div>
            <div className="text-[10px] text-on-surface/20 italic">
              {messages.length > 0 ? `${Math.floor(messages.length / 2)} exchanges` : 'Ready'}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
