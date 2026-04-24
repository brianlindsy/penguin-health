import { useState, useEffect, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'

// Resolve a single document's outstanding-work status, matching the logic
// used on ValidationRunDetailPage:
//   - needs_action: has failures, not all reviewed/fixed yet
//   - awaiting_staff: all failures reviewed (confirmed/fixed) but at least
//     one not yet fixed
//   - confirmed: no failures, or every failure has been fixed
function docStatus(doc) {
  const failed = doc.rules?.filter(r => r.status === 'FAIL') || []
  if (failed.length === 0) return 'confirmed'
  const allFixed = failed.every(r => r.fixed)
  if (allFixed) return 'confirmed'
  const allConfirmedOrFixed = failed.every(r => r.finding_confirmed || r.fixed)
  const anyNotFixed = failed.some(r => !r.fixed)
  if (allConfirmedOrFixed && anyNotFixed) return 'awaiting_staff'
  return 'needs_action'
}

// For category filter: the doc passes if any of its FAILs is tagged with
// one of the selected categories. Empty set = no category filter.
function docMatchesCategories(doc, categories) {
  if (!categories || categories.size === 0) return true
  return doc.rules?.some(r => r.status === 'FAIL' && r.category && categories.has(r.category)) || false
}

export function DashboardPage() {
  const { orgId } = useParams()
  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  // Which tile is expanded. null | 'needs_action' | 'awaiting_staff'
  const [expanded, setExpanded] = useState(null)
  const [programFilter, setProgramFilter] = useState('all')
  const [categoryFilter, setCategoryFilter] = useState(() => new Set())

  useEffect(() => {
    let cancelled = false
    api.listValidationRuns(orgId)
      .then(async resp => {
        const list = (resp?.runs || []).slice(0, 50)
        const withDetails = await Promise.all(
          list.map(run =>
            api.getValidationRun(orgId, run.validation_run_id)
              .then(detail => ({ ...run, ...detail }))
              .catch(() => ({ ...run, documents: [] }))
          )
        )
        if (cancelled) return
        setRuns(withDetails)
      })
      .catch(err => { if (!cancelled) setError(err.message) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [orgId])

  // Distinct programs + categories present across all runs — feeds the filter UI.
  const { availablePrograms, availableCategories } = useMemo(() => {
    const programs = new Set()
    const categories = new Set()
    runs.forEach(run => {
      run.documents?.forEach(doc => {
        if (doc.field_values?.program) programs.add(doc.field_values.program)
        doc.rules?.forEach(r => {
          if (r.category) categories.add(r.category)
        })
      })
    })
    return {
      availablePrograms: Array.from(programs).sort(),
      availableCategories: Array.from(categories).sort(),
    }
  }, [runs])

  // Aggregate per-run and grand totals for both status buckets, respecting
  // the program + category filters.
  const aggregates = useMemo(() => {
    let totalNeedsAction = 0
    let totalAwaitingStaff = 0
    const byRun = []

    runs.forEach(run => {
      let needsAction = 0
      let awaitingStaff = 0
      run.documents?.forEach(doc => {
        if (programFilter !== 'all' && doc.field_values?.program !== programFilter) return
        if (!docMatchesCategories(doc, categoryFilter)) return
        const status = docStatus(doc)
        if (status === 'needs_action') { needsAction += 1; totalNeedsAction += 1 }
        else if (status === 'awaiting_staff') { awaitingStaff += 1; totalAwaitingStaff += 1 }
      })
      if (needsAction > 0 || awaitingStaff > 0) {
        byRun.push({
          validation_run_id: run.validation_run_id,
          timestamp: run.timestamp,
          needsAction,
          awaitingStaff,
        })
      }
    })
    return { totalNeedsAction, totalAwaitingStaff, byRun }
  }, [runs, programFilter, categoryFilter])

  const toggleCategory = (cat) => {
    setCategoryFilter(prev => {
      const next = new Set(prev)
      if (next.has(cat)) next.delete(cat)
      else next.add(cat)
      return next
    })
  }

  const runsNeedsAction = aggregates.byRun
    .filter(r => r.needsAction > 0)
    .sort((a, b) => b.needsAction - a.needsAction)
  const runsAwaiting = aggregates.byRun
    .filter(r => r.awaitingStaff > 0)
    .sort((a, b) => b.awaitingStaff - a.awaitingStaff)

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="mb-5">
          <h1 className="text-2xl font-semibold text-gray-900">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-1">
            Outstanding notes across every validation run. Click a tile to see which runs still have work.
          </p>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-3 mb-5 flex-wrap">
          {availablePrograms.length > 0 && (
            <div className="inline-flex items-center gap-1.5 bg-white border border-gray-200 rounded-full pl-3 pr-1 py-0.5 shadow-sm">
              <svg className="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
              </svg>
              <select
                value={programFilter}
                onChange={(e) => setProgramFilter(e.target.value)}
                className="text-xs font-medium text-gray-700 bg-transparent border-0 focus:outline-none focus:ring-0 pr-1 py-1 cursor-pointer"
              >
                <option value="all">All programs</option>
                {availablePrograms.map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
          )}

          {availableCategories.length > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xs font-medium text-gray-400 uppercase tracking-wide">Category</span>
              {availableCategories.map(cat => {
                const active = categoryFilter.has(cat)
                return (
                  <button
                    key={cat}
                    onClick={() => toggleCategory(cat)}
                    aria-pressed={active}
                    className={`px-3 py-1 rounded-full text-xs font-medium border transition-colors ${
                      active
                        ? 'bg-blue-600 text-white border-blue-600'
                        : 'bg-white text-gray-700 border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    {cat}
                  </button>
                )
              })}
              {categoryFilter.size > 0 && (
                <button
                  onClick={() => setCategoryFilter(new Set())}
                  className="text-xs text-blue-600 hover:text-blue-800"
                >
                  Clear
                </button>
              )}
            </div>
          )}
        </div>

        {error && <p className="text-red-600 mb-4">Error: {error}</p>}
        {loading ? (
          <p className="text-gray-500">Loading dashboard…</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <StatusTile
              title="Needs Action"
              description="Notes with failed rules that still need review."
              count={aggregates.totalNeedsAction}
              tone="red"
              expanded={expanded === 'needs_action'}
              onToggle={() => setExpanded(expanded === 'needs_action' ? null : 'needs_action')}
              runs={runsNeedsAction}
              countKey="needsAction"
              orgId={orgId}
            />
            <StatusTile
              title="Awaiting Staff"
              description="Notes reviewed/confirmed but not yet fixed by staff."
              count={aggregates.totalAwaitingStaff}
              tone="amber"
              expanded={expanded === 'awaiting_staff'}
              onToggle={() => setExpanded(expanded === 'awaiting_staff' ? null : 'awaiting_staff')}
              runs={runsAwaiting}
              countKey="awaitingStaff"
              orgId={orgId}
            />
          </div>
        )}
      </div>
    </OrgWorkspaceLayout>
  )
}

function StatusTile({ title, description, count, tone, expanded, onToggle, runs, countKey, orgId }) {
  const toneClasses = tone === 'red' ? {
    border: 'border-red-200',
    bg: 'bg-red-50',
    accent: 'text-red-700',
    value: 'text-red-600',
    hover: 'hover:bg-red-100',
  } : {
    border: 'border-amber-200',
    bg: 'bg-amber-50',
    accent: 'text-amber-700',
    value: 'text-amber-600',
    hover: 'hover:bg-amber-100',
  }

  return (
    <div className={`rounded-xl border-2 ${toneClasses.border} bg-white overflow-hidden shadow-sm`}>
      <button
        onClick={onToggle}
        className={`w-full text-left p-5 transition-colors ${toneClasses.bg} ${toneClasses.hover}`}
        aria-expanded={expanded}
      >
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className={`text-xs font-semibold uppercase tracking-wider ${toneClasses.accent}`}>
              {title}
            </div>
            <div className={`text-5xl font-bold ${toneClasses.value} tabular-nums mt-1 leading-none`}>
              {count}
            </div>
            <p className="text-xs text-gray-600 mt-2">{description}</p>
            <p className="text-xs text-gray-400 mt-0.5">
              {runs.length} {runs.length === 1 ? 'run' : 'runs'} affected
              {runs.length > 0 ? ' — click to view' : ''}
            </p>
          </div>
          <svg
            className={`w-5 h-5 text-gray-500 transition-transform flex-shrink-0 ${expanded ? 'rotate-180' : ''}`}
            fill="none" stroke="currentColor" viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {expanded && (
        <div className="border-t border-gray-200">
          {runs.length === 0 ? (
            <p className="px-5 py-4 text-sm text-gray-500 italic">
              No runs have outstanding {title.toLowerCase()} items with the current filters.
            </p>
          ) : (
            <ul className="divide-y divide-gray-100">
              {runs.map(r => (
                <li key={r.validation_run_id}>
                  <Link
                    to={`/organizations/${orgId}/validation-runs/${r.validation_run_id}`}
                    className="flex items-center justify-between gap-3 px-5 py-3 hover:bg-gray-50 transition-colors"
                  >
                    <div className="min-w-0">
                      <div className="text-sm font-medium text-gray-900">
                        {r.timestamp ? new Date(r.timestamp).toLocaleDateString() : 'Unknown date'}
                      </div>
                      <div className="text-xs font-mono text-gray-400 truncate mt-0.5">
                        {r.validation_run_id}
                      </div>
                    </div>
                    <div className="flex items-center gap-2 flex-shrink-0">
                      <span className={`text-lg font-bold tabular-nums ${toneClasses.value}`}>
                        {r[countKey]}
                      </span>
                      <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                      </svg>
                    </div>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  )
}
