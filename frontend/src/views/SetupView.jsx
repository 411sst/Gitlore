import { useState } from 'react'
import API from '../config/api'

export default function SetupView({ onIngestionStart }) {
  const [repoUrl, setRepoUrl] = useState('')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [githubToken, setGithubToken] = useState('')
  const [subdir, setSubdir] = useState('')
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function handleSubmit(e) {
    e.preventDefault()
    if (!repoUrl.trim()) {
      setError('Repository URL is required.')
      return
    }
    setError('')
    setLoading(true)

    try {
      const todayIso = new Date().toISOString().slice(0, 10)
      const resolvedStartDate = startDate || '1970-01-01'
      const resolvedEndDate = endDate || todayIso

      const body = { repo_url: repoUrl.trim() }
      body.start_date = resolvedStartDate
      body.end_date = resolvedEndDate
      if (githubToken.trim()) body.github_token = githubToken.trim()
      if (subdir.trim()) body.subdir = subdir.trim()

      const res = await fetch(`${API}/ingest/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })

      if (!res.ok) {
        const data = await res.json().catch(() => ({}))
        throw new Error(data.detail || `Server error ${res.status}`)
      }

      const data = await res.json()
      onIngestionStart({
        repoUrl: repoUrl.trim(),
        startDate: resolvedStartDate,
        endDate: resolvedEndDate,
        sessionId: data.session_id,
      })
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex flex-col text-on-surface bg-[#0D0D0D]">
      {/* Main Content */}
      <main className="flex-1 flex items-center justify-center p-6">
        <div className="w-[520px] bg-[#1A1A1A] px-[40px] py-[48px] rounded-lg border border-outline-variant/10 shadow-2xl">

          {/* Brand */}
          <div className="mb-6">
            <span className="text-[#F5A623] font-semibold text-lg tracking-tight">Gitlore</span>
          </div>

          {/* Heading */}
          <header className="mb-8">
            <h1 className="text-[22px] font-medium text-on-surface leading-tight">Onboard a repository</h1>
            <p className="text-on-surface-variant text-sm mt-1">Ask anything about any codebase</p>
          </header>

          <hr className="border-t border-outline-variant/20 mb-8" />

          {/* Form */}
          <form className="space-y-6" onSubmit={handleSubmit}>

            {/* Repository URL */}
            <div className="space-y-2">
              <label className="block text-[0.75rem] font-medium text-on-surface-variant uppercase tracking-widest">
                Repository URL
              </label>
              <input
                type="text"
                value={repoUrl}
                onChange={e => setRepoUrl(e.target.value)}
                placeholder="https://github.com/etcd-io/etcd"
                className="w-full bg-[#242424] border border-outline-variant/30 text-on-surface font-mono text-sm px-4 py-3 rounded-lg focus:ring-1 focus:ring-primary-container focus:outline-none transition-all placeholder:text-on-surface-variant/40 placeholder:font-mono"
              />
            </div>

            {/* Date Range */}
            <div className="flex gap-4">
              <div className="flex-1 space-y-2">
                <label className="block text-[0.75rem] font-medium text-on-surface-variant uppercase tracking-widest">
                  From
                </label>
                <input
                  type="date"
                  value={startDate}
                  onChange={e => setStartDate(e.target.value)}
                  className="w-full bg-[#242424] border border-outline-variant/30 text-on-surface text-sm px-4 py-3 rounded-lg focus:ring-1 focus:ring-primary-container focus:outline-none transition-all appearance-none [color-scheme:dark]"
                />
              </div>
              <div className="flex-1 space-y-2">
                <label className="block text-[0.75rem] font-medium text-on-surface-variant uppercase tracking-widest">
                  To
                </label>
                <input
                  type="date"
                  value={endDate}
                  onChange={e => setEndDate(e.target.value)}
                  className="w-full bg-[#242424] border border-outline-variant/30 text-on-surface text-sm px-4 py-3 rounded-lg focus:ring-1 focus:ring-primary-container focus:outline-none transition-all appearance-none [color-scheme:dark]"
                />
              </div>
            </div>

            {/* Advanced Options */}
            <button
              type="button"
              onClick={() => setShowAdvanced(v => !v)}
              className="flex items-center justify-between w-full group py-2"
            >
              <span className="text-sm font-medium text-on-surface-variant group-hover:text-on-surface transition-colors">
                Advanced options
              </span>
              <span className="material-symbols-outlined text-on-surface-variant group-hover:text-on-surface transition-colors text-[20px]">
                {showAdvanced ? 'expand_less' : 'expand_more'}
              </span>
            </button>

            {showAdvanced && (
              <div className="space-y-4 pt-1">
                <div className="space-y-2">
                  <label className="block text-[0.75rem] font-medium text-on-surface-variant uppercase tracking-widest">
                    GitHub Token <span className="normal-case tracking-normal text-on-surface-variant/50">(optional)</span>
                  </label>
                  <input
                    type="password"
                    value={githubToken}
                    onChange={e => setGithubToken(e.target.value)}
                    placeholder="ghp_..."
                    className="w-full bg-[#242424] border border-outline-variant/30 text-on-surface font-mono text-sm px-4 py-3 rounded-lg focus:ring-1 focus:ring-primary-container focus:outline-none transition-all placeholder:text-on-surface-variant/40 placeholder:font-mono"
                  />
                </div>
                <div className="space-y-2">
                  <label className="block text-[0.75rem] font-medium text-on-surface-variant uppercase tracking-widest">
                    Subdirectory <span className="normal-case tracking-normal text-on-surface-variant/50">(optional)</span>
                  </label>
                  <input
                    type="text"
                    value={subdir}
                    onChange={e => setSubdir(e.target.value)}
                    placeholder="src/backend"
                    className="w-full bg-[#242424] border border-outline-variant/30 text-on-surface font-mono text-sm px-4 py-3 rounded-lg focus:ring-1 focus:ring-primary-container focus:outline-none transition-all placeholder:text-on-surface-variant/40 placeholder:font-mono"
                  />
                </div>
              </div>
            )}

            {/* Error */}
            {error && (
              <p className="text-error text-sm font-mono">{error}</p>
            )}

            {/* Submit */}
            <div className="pt-4">
              <button
                type="submit"
                disabled={loading}
                className="w-full bg-primary-container text-[#0D0D0D] font-semibold py-4 rounded-lg hover:brightness-110 active:scale-[0.98] transition-all flex justify-center items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
              >
                {loading ? (
                  <>
                    <span className="material-symbols-outlined text-[18px] animate-spin">progress_activity</span>
                    Starting…
                  </>
                ) : (
                  'Start ingestion'
                )}
              </button>
            </div>
          </form>
        </div>
      </main>

      {/* Footer */}
      <footer className="w-full flex flex-col md:flex-row justify-between items-center px-8 py-6 gap-4 border-t border-outline-variant/10 bg-[#0D0D0D]">
        <div className="font-mono text-[0.75rem] uppercase tracking-widest text-[#E5E2E1]/60">
          © 2026 Gitlore. Intellectual Rigor Assured.
        </div>
        <div className="flex gap-6">
          <a href="#" className="font-mono text-[0.75rem] uppercase tracking-widest text-[#E5E2E1]/60 hover:text-[#E5E2E1] transition-colors">Documentation</a>
          <a href="#" className="font-mono text-[0.75rem] uppercase tracking-widest text-[#E5E2E1]/60 hover:text-[#E5E2E1] transition-colors">Changelog</a>
          <a href="#" className="font-mono text-[0.75rem] uppercase tracking-widest text-[#E5E2E1]/60 hover:text-[#E5E2E1] transition-colors">Privacy</a>
        </div>
      </footer>
    </div>
  )
}
