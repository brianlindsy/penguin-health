import { useState, useEffect, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'

const getCredibleLink = (documentId) =>
  `https://www.cbh3.crediblebh.com/visit/clientvisit_view.asp?clientvisit_id=${documentId}&provportal=0`

// Revenue Analysis — breaks "revenue at risk" (the rate on any failed note)
// down by program, CPT code, diagnosis code, and staff. Pulls recent
// validation runs and aggregates failed-doc rates across the selected
// date window.
export function RevenueAnalysisPage() {
  const { orgId } = useParams()
  const [runs, setRuns] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [periodFilter, setPeriodFilter] = useState('all')
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')

  useEffect(() => {
    api.listValidationRuns(orgId)
      .then(async (resp) => {
        const list = (Array.isArray(resp) ? resp : resp?.runs) || []
        const recent = list.slice(0, 10) // cap parallel fetches
        const withDetails = await Promise.all(
          recent.map(async run => ({
            ...(await api.getValidationRun(orgId, run.validation_run_id)),
            validation_run_id: run.validation_run_id,
            timestamp: run.timestamp,
          }))
        )
        setRuns(withDetails)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  // Aggregate failed-doc rates by the four dimensions, respecting period filter.
  const analysis = useMemo(() => {
    if (!runs) return null

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
    const inWindow = (run) => {
      if (startCutoff == null && endCutoff == null) return true
      if (!run.timestamp) return true
      const t = new Date(run.timestamp).getTime()
      if (startCutoff != null && t < startCutoff) return false
      if (endCutoff != null && t >= endCutoff) return false
      return true
    }

    // Each map tracks { amount, docs: [{ doc, runId, rate }] } per key so we
    // can surface the contributing notes when the user clicks an entry.
    const byProgram = new Map()
    const byCpt = new Map()
    const byDx = new Map()
    const byStaff = new Map()
    let totalRevenue = 0
    let blockedClaims = 0

    const addTo = (map, key, entry) => {
      if (!map.has(key)) map.set(key, { amount: 0, docs: [] })
      const bucket = map.get(key)
      bucket.amount += entry.rate
      bucket.docs.push(entry)
    }

    runs.filter(inWindow).forEach(run => {
      run.documents?.forEach(doc => {
        if (!(doc.summary?.failed > 0)) return
        const rate = parseFloat(doc.field_values?.rate) || 0
        if (!rate) return

        totalRevenue += rate
        blockedClaims += 1

        const program = doc.field_values?.program || 'Unknown'
        const cpt = doc.field_values?.cpt_code || 'Unknown'
        const dx = doc.field_values?.diagnosis_code || 'Unknown'
        const staff = doc.field_values?.employee_name || 'Unknown'

        const entry = { doc, runId: run.validation_run_id, rate }
        addTo(byProgram, program, entry)
        addTo(byCpt, cpt, entry)
        addTo(byDx, dx, entry)
        addTo(byStaff, staff, entry)
      })
    })

    const toSortedEntries = (map) =>
      Array.from(map.entries())
        .map(([name, { amount, docs }]) => ({
          name,
          amount,
          // Most expensive note first so clicking a row surfaces the biggest
          // drivers at the top of the expanded list.
          docs: [...docs].sort((a, b) => b.rate - a.rate),
        }))
        .sort((a, b) => b.amount - a.amount)

    return {
      totalRevenue,
      blockedClaims,
      byProgram: toSortedEntries(byProgram),
      byCpt: toSortedEntries(byCpt),
      byDx: toSortedEntries(byDx),
      byStaff: toSortedEntries(byStaff),
    }
  }, [runs, periodFilter, customStartDate, customEndDate])

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="mb-5">
          <h1 className="text-2xl font-semibold text-gray-900">Revenue Analysis</h1>
          <p className="text-sm text-gray-500 mt-1">
            What's driving revenue at risk, broken down by program, CPT code, diagnosis code, and staff.
          </p>
        </div>

        {/* Filter bar */}
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
        {loading && <p className="text-gray-500">Loading revenue data...</p>}
        {!loading && !error && analysis && (
          <>
            {/* Headline stats */}
            <div className="grid grid-cols-3 gap-3 mb-5">
              <HeadlineStat
                label="Total revenue at risk"
                value={formatCurrency(analysis.totalRevenue)}
                tone="red"
              />
              <HeadlineStat
                label="Blocked claims"
                value={analysis.blockedClaims.toLocaleString()}
              />
              <HeadlineStat
                label="Avg per blocked claim"
                value={analysis.blockedClaims > 0
                  ? formatCurrency(analysis.totalRevenue / analysis.blockedClaims)
                  : '—'}
              />
            </div>

            {/* Breakdowns */}
            {analysis.blockedClaims === 0 ? (
              <div className="bg-white border border-gray-200 rounded-xl p-8 text-center text-gray-500">
                No revenue at risk in the selected window.
              </div>
            ) : (
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                <BreakdownCard
                  title="Top programs"
                  entries={analysis.byProgram}
                  totalRevenue={analysis.totalRevenue}
                  orgId={orgId}
                />
                <BreakdownCard
                  title="Top CPT codes"
                  entries={analysis.byCpt}
                  totalRevenue={analysis.totalRevenue}
                  orgId={orgId}
                  valueMono
                />
                <BreakdownCard
                  title="Top diagnosis codes"
                  entries={analysis.byDx}
                  totalRevenue={analysis.totalRevenue}
                  orgId={orgId}
                  valueMono
                />
                <BreakdownCard
                  title="Top staff"
                  entries={analysis.byStaff}
                  totalRevenue={analysis.totalRevenue}
                  orgId={orgId}
                />
              </div>
            )}
          </>
        )}
      </div>
    </OrgWorkspaceLayout>
  )
}

function HeadlineStat({ label, value, tone }) {
  const toneClass = tone === 'red' ? 'text-red-600' : 'text-gray-900'
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 shadow-sm">
      <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider">{label}</div>
      <div className={`text-2xl font-bold ${toneClass} mt-1`}>{value}</div>
    </div>
  )
}

function BreakdownCard({ title, entries, totalRevenue, valueMono = false, orgId }) {
  const TOP_N = 8
  const top = entries.slice(0, TOP_N)
  const rest = entries.slice(TOP_N)
  const restAmount = rest.reduce((sum, e) => sum + e.amount, 0)
  const hasRest = rest.length > 0
  // Only one row expands at a time per card.
  const [expandedName, setExpandedName] = useState(null)

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
      <div className="flex items-baseline justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-900 uppercase tracking-wide">{title}</h3>
        <span className="text-xs text-gray-400">
          {entries.length} {entries.length === 1 ? 'item' : 'items'}
        </span>
      </div>
      <div className="space-y-3">
        {top.map(entry => {
          const pct = totalRevenue > 0 ? (entry.amount / totalRevenue) * 100 : 0
          const isOpen = expandedName === entry.name
          const toggle = () => setExpandedName(isOpen ? null : entry.name)
          return (
            <div key={entry.name}>
              <button
                onClick={toggle}
                className={`w-full text-left rounded-md -mx-2 px-2 py-1 transition-colors ${
                  isOpen ? 'bg-gray-50' : 'hover:bg-gray-50'
                }`}
                aria-expanded={isOpen}
              >
                <div className="flex items-baseline justify-between gap-2 mb-1">
                  <span className={`text-sm text-gray-700 truncate flex items-center gap-1 ${valueMono ? 'font-mono' : ''}`} title={entry.name}>
                    <svg
                      className={`w-3 h-3 text-gray-400 transition-transform flex-shrink-0 ${isOpen ? 'rotate-90' : ''}`}
                      fill="none" stroke="currentColor" viewBox="0 0 24 24"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                    {entry.name}
                  </span>
                  <span className="text-sm font-semibold text-gray-900 tabular-nums whitespace-nowrap">
                    {formatCurrency(entry.amount)}
                    <span className="text-xs font-normal text-gray-400 ml-2">{Math.round(pct)}%</span>
                  </span>
                </div>
                <div className="w-full h-1.5 bg-gray-100 rounded-full overflow-hidden">
                  <div className="bg-red-500 h-full rounded-full" style={{ width: `${pct}%` }} />
                </div>
              </button>

              {isOpen && (
                <ContributingNotesList
                  docs={entry.docs}
                  orgId={orgId}
                />
              )}
            </div>
          )
        })}
        {hasRest && (
          <div className="pt-2 border-t border-gray-100 flex items-baseline justify-between text-xs text-gray-500">
            <span>{rest.length} more</span>
            <span className="tabular-nums">{formatCurrency(restAmount)}</span>
          </div>
        )}
      </div>
    </div>
  )
}

function ContributingNotesList({ docs, orgId }) {
  const MAX_ROWS = 10
  const [showAll, setShowAll] = useState(false)
  const visible = showAll ? docs : docs.slice(0, MAX_ROWS)

  if (docs.length === 0) {
    return (
      <p className="mt-2 ml-4 text-xs text-gray-400 italic">No notes available.</p>
    )
  }

  return (
    <div className="mt-2 ml-4 pl-4 border-l-2 border-gray-100 space-y-1">
      {visible.map(({ doc, runId, rate }, idx) => {
        const employee = doc.field_values?.employee_name
        const program = doc.field_values?.program
        const cpt = doc.field_values?.cpt_code
        const date = doc.field_values?.date
        return (
          <div
            key={`${doc.document_id}-${idx}`}
            className="flex items-center justify-between gap-3 py-1.5 text-xs"
          >
            <div className="min-w-0 flex-1 flex items-center gap-2 flex-wrap">
              <a
                href={getCredibleLink(doc.document_id)}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:text-blue-800 hover:underline font-mono flex-shrink-0"
                onClick={(e) => e.stopPropagation()}
              >
                #{doc.document_id}
              </a>
              {employee && <span className="text-gray-700 truncate">{employee}</span>}
              {program && <span className="text-gray-400">· {program}</span>}
              {cpt && <span className="text-gray-400">· CPT {cpt}</span>}
              {date && <span className="text-gray-400">· {date}</span>}
            </div>
            <div className="flex items-center gap-3 flex-shrink-0">
              <span className="text-gray-900 font-semibold tabular-nums">{formatCurrency(rate)}</span>
              {runId && (
                <Link
                  to={`/organizations/${orgId}/validation-runs/${runId}?doc=${doc.document_id}`}
                  className="text-blue-600 hover:text-blue-800 hover:underline"
                  onClick={(e) => e.stopPropagation()}
                >
                  View run
                </Link>
              )}
            </div>
          </div>
        )
      })}
      {docs.length > MAX_ROWS && (
        <button
          onClick={() => setShowAll(!showAll)}
          className="text-xs text-blue-600 hover:text-blue-800 pt-1"
        >
          {showAll ? 'Show less' : `Show ${docs.length - MAX_ROWS} more`}
        </button>
      )}
    </div>
  )
}

function formatCurrency(n) {
  return `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}
