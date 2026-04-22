import { useState, useEffect, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'

// Customer-facing Validation Results list. Reuses the runs from
// api.listValidationRuns, but replaces the dense admin table with a card
// layout, summary stats, and an inline date filter.
export function ValidationResultsPage() {
  const { orgId } = useParams()
  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [periodFilter, setPeriodFilter] = useState('all')
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')

  useEffect(() => {
    api.listValidationRuns(orgId)
      .then(data => setRuns(data.runs || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  // Apply date window filter based on the run's own timestamp.
  const filteredRuns = useMemo(() => {
    const dayMs = 24 * 60 * 60 * 1000
    const now = Date.now()
    let startCutoff = null
    let endCutoff = null
    if (periodFilter === '24h') startCutoff = now - dayMs
    else if (periodFilter === '7d') startCutoff = now - 7 * dayMs
    else if (periodFilter === '30d') startCutoff = now - 30 * dayMs
    else if (periodFilter === '90d') startCutoff = now - 90 * dayMs
    else if (periodFilter === 'custom') {
      if (customStartDate) startCutoff = new Date(customStartDate).getTime()
      if (customEndDate) endCutoff = new Date(customEndDate).getTime() + dayMs
    }
    if (startCutoff == null && endCutoff == null) return runs
    return runs.filter(r => {
      if (!r.timestamp) return true
      const t = new Date(r.timestamp).getTime()
      if (startCutoff != null && t < startCutoff) return false
      if (endCutoff != null && t >= endCutoff) return false
      return true
    })
  }, [runs, periodFilter, customStartDate, customEndDate])

  // Ordered newest first, same as the admin table used to render.
  const sortedRuns = useMemo(() => {
    return [...filteredRuns].sort((a, b) => {
      const at = a.timestamp ? new Date(a.timestamp).getTime() : 0
      const bt = b.timestamp ? new Date(b.timestamp).getTime() : 0
      return bt - at
    })
  }, [filteredRuns])

  // High-level numbers for the summary strip at the top.
  const summary = useMemo(() => {
    const total = filteredRuns.length
    let docs = 0, pass = 0, fail = 0, skip = 0
    filteredRuns.forEach(r => {
      docs += r.total_documents || 0
      pass += r.passed || 0
      fail += r.failed || 0
      skip += r.skipped || 0
    })
    const latest = sortedRuns[0]
    return { total, docs, pass, fail, skip, latest }
  }, [filteredRuns, sortedRuns])

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="mb-5">
          <h1 className="text-2xl font-semibold text-gray-900">Validation Results</h1>
          <p className="text-sm text-gray-500 mt-1">
            {sortedRuns.length}
            {sortedRuns.length !== runs.length ? ` of ${runs.length}` : ''}
            {' '}{sortedRuns.length === 1 ? 'run' : 'runs'}
            {periodFilter !== 'all' ? ' in the selected window' : ''}
          </p>
        </div>

        {/* Summary strip */}
        {!loading && !error && filteredRuns.length > 0 && (
          <div className="grid grid-cols-4 gap-3 mb-5">
            <SummaryCard label="Runs" value={summary.total} />
            <SummaryCard label="Documents" value={summary.docs} />
            <SummaryCard label="Failures" value={summary.fail} tone="red" />
            <SummaryCard
              label="Most recent"
              value={summary.latest?.timestamp ? formatRelative(summary.latest.timestamp) : '—'}
              subtext={summary.latest?.timestamp ? new Date(summary.latest.timestamp).toLocaleString() : null}
              valueClassName="text-base"
            />
          </div>
        )}

        {/* Filter bar — compact pill, matching audit rules / staff performance */}
        <div className="flex items-center gap-2 mb-5 flex-wrap">
          <span className="inline-flex items-center gap-1.5 text-xs font-medium text-gray-400 uppercase tracking-wide">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
            </svg>
            Filters
          </span>
          <div className="inline-flex items-center gap-1.5 bg-white border border-gray-200 rounded-full pl-3 pr-1 py-0.5 shadow-sm">
            <svg className="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
            </svg>
            <select
              value={periodFilter}
              onChange={(e) => setPeriodFilter(e.target.value)}
              className="text-xs font-medium text-gray-700 bg-transparent border-0 focus:outline-none focus:ring-0 pr-1 py-1 cursor-pointer"
            >
              <option value="all">All time</option>
              <option value="24h">Last 24 hours</option>
              <option value="7d">Last 7 days</option>
              <option value="30d">Last 30 days</option>
              <option value="90d">Last 90 days</option>
              <option value="custom">Custom range</option>
            </select>
          </div>
          {periodFilter !== 'all' && (
            <button
              onClick={() => { setPeriodFilter('all'); setCustomStartDate(''); setCustomEndDate('') }}
              className="text-xs text-blue-600 hover:text-blue-800"
            >
              Clear
            </button>
          )}
        </div>

        {periodFilter === 'custom' && (
          <div className="flex items-end gap-3 mb-5 flex-wrap">
            <div className="flex flex-col">
              <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">From</label>
              <input
                type="date"
                value={customStartDate}
                onChange={(e) => setCustomStartDate(e.target.value)}
                className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div className="flex flex-col">
              <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">To</label>
              <input
                type="date"
                value={customEndDate}
                onChange={(e) => setCustomEndDate(e.target.value)}
                className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>
        )}

        {error && <p className="text-red-600 mb-4">Error: {error}</p>}
        {loading ? (
          <p className="text-gray-500">Loading validation runs...</p>
        ) : runs.length === 0 ? (
          <p className="text-gray-500">No validation runs found for this organization.</p>
        ) : sortedRuns.length === 0 ? (
          <p className="text-gray-500">No validation runs in the selected date range.</p>
        ) : (
          <div className="space-y-3">
            {sortedRuns.map(run => (
              <RunCard key={run.validation_run_id} orgId={orgId} run={run} />
            ))}
          </div>
        )}
      </div>
    </OrgWorkspaceLayout>
  )
}

function SummaryCard({ label, value, subtext, tone, valueClassName }) {
  const toneClass = tone === 'red' ? 'text-red-600' : 'text-gray-900'
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-3 shadow-sm">
      <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider">{label}</div>
      <div className={`${valueClassName || 'text-2xl'} font-bold ${toneClass} mt-0.5`}>{value}</div>
      {subtext && <div className="text-[10px] text-gray-400 mt-0.5 truncate">{subtext}</div>}
    </div>
  )
}

function RunCard({ orgId, run }) {
  const total = run.total_documents || 0
  const passed = run.passed || 0
  const failed = run.failed || 0
  const skipped = run.skipped || 0
  const processed = Math.max(passed + failed + skipped, 1)
  const passPct = (passed / processed) * 100
  const failPct = (failed / processed) * 100
  const skipPct = (skipped / processed) * 100
  const overallStatus = failed > 0 ? 'FAIL' : 'PASS'

  const when = run.timestamp ? new Date(run.timestamp) : null

  return (
    <Link
      to={`/organizations/${orgId}/validation-runs/${run.validation_run_id}`}
      className="block bg-white rounded-xl border border-gray-200 shadow-sm p-5 hover:shadow-md hover:border-gray-300 transition-all"
    >
      <div className="flex items-start justify-between gap-4 mb-3">
        <div className="min-w-0">
          <div className="flex items-baseline gap-3 flex-wrap">
            <span className="text-base font-semibold text-gray-900">
              {when ? when.toLocaleString() : 'No timestamp'}
            </span>
            {when && (
              <span className="text-xs text-gray-500">{formatRelative(run.timestamp)}</span>
            )}
          </div>
          <div className="text-xs font-mono text-gray-400 mt-0.5 truncate">
            {run.validation_run_id}
          </div>
        </div>
        <StatusPill status={overallStatus} />
      </div>

      {/* Stacked pass/fail/skip progress bar */}
      <div className="w-full h-2 bg-gray-100 rounded-full overflow-hidden flex mb-3">
        {passPct > 0 && <div className="bg-green-500" style={{ width: `${passPct}%` }} />}
        {failPct > 0 && <div className="bg-red-500" style={{ width: `${failPct}%` }} />}
        {skipPct > 0 && <div className="bg-gray-300" style={{ width: `${skipPct}%` }} />}
      </div>

      {/* Count chips */}
      <div className="flex items-center gap-4 text-sm">
        <CountChip label="Docs" value={total} color="gray" />
        <CountChip label="Pass" value={passed} color="green" />
        <CountChip label="Fail" value={failed} color="red" />
        <CountChip label="Skip" value={skipped} color="neutral" />
      </div>
    </Link>
  )
}

function CountChip({ label, value, color }) {
  const colors = {
    green: 'text-green-700',
    red: 'text-red-700',
    gray: 'text-gray-900',
    neutral: 'text-gray-600',
  }
  return (
    <div className="flex items-baseline gap-1.5">
      <span className={`text-lg font-bold tabular-nums ${colors[color]}`}>{value}</span>
      <span className="text-xs uppercase tracking-wide text-gray-400">{label}</span>
    </div>
  )
}

function StatusPill({ status }) {
  const isFail = status === 'FAIL'
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold flex-shrink-0 ${
        isFail
          ? 'bg-red-50 text-red-700 border border-red-200'
          : 'bg-green-50 text-green-700 border border-green-200'
      }`}
    >
      <span className={`w-1.5 h-1.5 rounded-full ${isFail ? 'bg-red-500' : 'bg-green-500'}`} />
      {isFail ? 'Fail' : 'Pass'}
    </span>
  )
}

// Cheap relative-time helper: "2 min ago", "3 hr ago", "yesterday", "5 days ago".
// Falls back to a full date after a week.
function formatRelative(ts) {
  const then = new Date(ts).getTime()
  if (Number.isNaN(then)) return ''
  const diff = Date.now() - then
  const mins = Math.round(diff / 60000)
  if (mins < 1) return 'just now'
  if (mins < 60) return `${mins} min ago`
  const hours = Math.round(mins / 60)
  if (hours < 24) return `${hours} hr ago`
  const days = Math.round(hours / 24)
  if (days === 1) return 'yesterday'
  if (days < 7) return `${days} days ago`
  return new Date(ts).toLocaleDateString()
}
