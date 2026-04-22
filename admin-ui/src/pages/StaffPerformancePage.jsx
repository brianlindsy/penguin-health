import { useState, useEffect, useMemo } from 'react'
import { useParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'

// Generate link to Credible BH for a document ID
const getCredibleLink = (documentId) =>
  `https://www.cbh3.crediblebh.com/visit/clientvisit_view.asp?clientvisit_id=${documentId}&provportal=0`

export function StaffPerformancePage() {
  const { orgId } = useParams()
  const [data, setData] = useState(null)
  const [ruleDefinitions, setRuleDefinitions] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [selectedStaff, setSelectedStaff] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [periodFilter, setPeriodFilter] = useState('all')
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')
  const [sortOrder, setSortOrder] = useState('asc') // 'asc' = worst first, 'desc' = best first

  useEffect(() => {
    // Load rule definitions + validation runs in parallel. Rule definitions
    // are the authoritative source for rule.category (same as the admin table).
    Promise.all([
      api.listRules(orgId),
      api.listValidationRuns(orgId).then(async (runsData) => {
        const runList = runsData.runs.slice(0, 10)
        return Promise.all(
          runList.map(async run => ({
            ...(await api.getValidationRun(orgId, run.validation_run_id)),
            validation_run_id: run.validation_run_id,
            timestamp: run.timestamp,
          }))
        )
      }),
    ])
      .then(([rulesResponse, runsWithDetails]) => {
        setRuleDefinitions(Array.isArray(rulesResponse) ? rulesResponse : rulesResponse?.rules || [])
        setData(runsWithDetails)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  // rule_id -> category, drawn from the authoritative rule definitions.
  const ruleCategoryById = useMemo(() => {
    const map = new Map()
    ruleDefinitions.forEach(r => {
      if (r?.rule_id && r?.category) map.set(r.rule_id, r.category)
    })
    return map
  }, [ruleDefinitions])

  // Runs narrowed by the active date filter. Shared across the staff roll-up
  // and the Analytics view in ProgramSummaryView.
  const filteredRuns = useMemo(() => {
    if (!data) return []
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
      // endCutoff is exclusive — include the full end day
      if (customEndDate) endCutoff = new Date(customEndDate).getTime() + dayMs
    }
    if (startCutoff == null && endCutoff == null) return data
    return data.filter(run => {
      if (!run.timestamp) return true
      const t = new Date(run.timestamp).getTime()
      if (startCutoff != null && t < startCutoff) return false
      if (endCutoff != null && t >= endCutoff) return false
      return true
    })
  }, [data, periodFilter, customStartDate, customEndDate])

  // Aggregate staff performance from all validation runs
  const staffPerformance = useMemo(() => {
    if (!data) return []

    const staffMap = new Map()

    filteredRuns.forEach(run => {
      run.documents?.forEach(doc => {
        const employeeName = doc.field_values?.employee_name || 'Unknown'
        const program = doc.field_values?.program || 'Unknown'

        if (!staffMap.has(employeeName)) {
          staffMap.set(employeeName, {
            name: employeeName,
            program: program,
            totalRules: 0,
            passedRules: 0,
            failedRules: 0,
            skippedRules: 0,
            documents: [],
            failedDocuments: [],
            passedDocumentCount: 0,
            ruleFailures: new Map(), // Track which rules fail most often
          })
        }

        const staff = staffMap.get(employeeName)
        staff.totalRules += doc.summary?.total_rules || 0
        staff.passedRules += doc.summary?.passed || 0
        staff.failedRules += doc.summary?.failed || 0
        staff.skippedRules += doc.summary?.skipped || 0
        staff.documents.push(doc)

        const failedCount = doc.summary?.failed || 0
        const passedCount = doc.summary?.passed || 0
        if (failedCount > 0) {
          staff.failedDocuments.push(doc)
          // Track rule failures
          doc.rules?.forEach(rule => {
            if (rule.status === 'FAIL') {
              const count = staff.ruleFailures.get(rule.rule_name) || 0
              staff.ruleFailures.set(rule.rule_name, count + 1)
            }
          })
        } else if (passedCount > 0) {
          // Note counts as audited + passed only if it has at least one passing
          // rule and no failures (all-skip notes don't count as audited).
          staff.passedDocumentCount += 1
        }
      })
    })

    // Convert to array and calculate pass rates based on audited notes.
    return Array.from(staffMap.values())
      .map(staff => {
        const auditedDocs = staff.passedDocumentCount + staff.failedDocuments.length
        return {
          ...staff,
          auditedDocumentCount: auditedDocs,
          // null means: no audited notes (all skips / unaudited) — render as "-"
          passRate: auditedDocs > 0
            ? Math.round((staff.passedDocumentCount / auditedDocs) * 100)
            : null,
          errorCount: staff.failedRules,
          // Get top recurring failures
          recurringFailures: Array.from(staff.ruleFailures.entries())
            .sort((a, b) => b[1] - a[1])
            .slice(0, 5)
            .map(([name, count]) => ({ name, count })),
        }
      })
      // Sort worst first; push unaudited staff (null passRate) to the end.
      .sort((a, b) => {
        if (a.passRate == null && b.passRate == null) return 0
        if (a.passRate == null) return 1
        if (b.passRate == null) return -1
        return a.passRate - b.passRate
      })
  }, [data, filteredRuns])

  // Filter staff by search and apply the user-selected sort order. Null pass
  // rates (unaudited) always sink to the bottom regardless of direction.
  const filteredStaff = useMemo(() => {
    const search = searchTerm.toLowerCase()
    const list = search
      ? staffPerformance.filter(s =>
          s.name.toLowerCase().includes(search) ||
          s.program.toLowerCase().includes(search)
        )
      : staffPerformance
    return [...list].sort((a, b) => {
      if (a.passRate == null && b.passRate == null) return 0
      if (a.passRate == null) return 1
      if (b.passRate == null) return -1
      return sortOrder === 'asc' ? a.passRate - b.passRate : b.passRate - a.passRate
    })
  }, [staffPerformance, searchTerm, sortOrder])

  // Keep the selected staff's data in sync as filters change; drop the
  // Keep the selected staff's data in sync as filters change; drop the
  // selection (falling back to the summary view) if they disappear.
  useEffect(() => {
    if (!selectedStaff) return
    if (filteredStaff.length === 0) {
      setSelectedStaff(null)
      return
    }
    const match = filteredStaff.find(s => s.name === selectedStaff.name)
    if (match && match !== selectedStaff) {
      setSelectedStaff(match)
    } else if (!match) {
      setSelectedStaff(null)
    }
  }, [filteredStaff, selectedStaff])


  if (loading) {
    return (
      <OrgWorkspaceLayout>
        <div className="flex items-center justify-center h-64">
          <p className="text-gray-500">Loading staff performance data...</p>
        </div>
      </OrgWorkspaceLayout>
    )
  }

  if (error) {
    return <OrgWorkspaceLayout><div className="p-4"><p className="text-red-600">Error: {error}</p></div></OrgWorkspaceLayout>
  }

  return (
    <OrgWorkspaceLayout>
    <div className="flex gap-6">
      {/* Left Panel - Staff Standings */}
      <div className="w-96 flex flex-col bg-white rounded-lg shadow sticky top-4 self-start max-h-[calc(100vh-100px)]">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2 min-w-0">
              <h2 className="text-sm font-semibold text-blue-600 uppercase tracking-wide">
                Staff Standings
              </h2>
              <span className="text-xs bg-gray-100 text-gray-600 px-2 py-1 rounded truncate">
                {orgId}
              </span>
            </div>
            <button
              onClick={() => setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')}
              className="flex-shrink-0 p-1.5 rounded-md text-blue-600 hover:bg-blue-50 transition-colors"
              title={sortOrder === 'asc' ? 'Sort: low to high (click to reverse)' : 'Sort: high to low (click to reverse)'}
              aria-label="Toggle sort order"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
              </svg>
            </button>
          </div>
          <div className="relative">
            <input
              type="text"
              placeholder="Search staff..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-9 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <svg className="absolute left-3 top-2.5 h-4 w-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto">
          {filteredStaff.map(staff => (
            <StaffListItem
              key={staff.name}
              staff={staff}
              selected={selectedStaff?.name === staff.name}
              onClick={() => setSelectedStaff(staff)}
            />
          ))}
          {filteredStaff.length === 0 && (
            <p className="p-4 text-sm text-gray-500">No staff members found.</p>
          )}
        </div>
      </div>

      {/* Right Panel - Summary (default) or Staff Detail */}
      <div className="flex-1 flex flex-col">
        {selectedStaff ? (
          <StaffDetailPanel
            staff={selectedStaff}
            onBack={() => setSelectedStaff(null)}
            filterBar={
              <FilterBar
                periodFilter={periodFilter}
                onPeriodChange={setPeriodFilter}
                customStartDate={customStartDate}
                onCustomStartChange={setCustomStartDate}
                customEndDate={customEndDate}
                onCustomEndChange={setCustomEndDate}
                allStaff={staffPerformance}
                selectedStaffName={selectedStaff?.name || ''}
                onSelectStaffName={(name) => {
                  const staff = staffPerformance.find(s => s.name === name)
                  if (staff) setSelectedStaff(staff)
                }}
              />
            }
          />
        ) : (
          <ProgramSummaryView
            staffPerformance={staffPerformance}
            filteredRuns={filteredRuns}
            ruleDefinitions={ruleDefinitions}
            ruleCategoryById={ruleCategoryById}
            periodFilter={periodFilter}
            onPeriodChange={setPeriodFilter}
            customStartDate={customStartDate}
            onCustomStartChange={setCustomStartDate}
            customEndDate={customEndDate}
            onCustomEndChange={setCustomEndDate}
            onSelectStaff={setSelectedStaff}
            onSelectProgram={(program) => setSearchTerm(program)}
          />
        )}
      </div>
    </div>
    </OrgWorkspaceLayout>
  )
}


// Business-hours math. "48 business hours" = 48 hours excluding weekends,
// treating each weekday as 24 available hours. Iterates day by day so it's
// fast enough even over long ranges.
function businessHoursBetween(start, end) {
  const a = start instanceof Date ? start : new Date(start)
  const b = end instanceof Date ? end : new Date(end)
  if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return null
  if (b <= a) return 0
  const msPerHour = 60 * 60 * 1000
  let total = 0
  const cur = new Date(a)
  while (cur < b) {
    const dayEnd = new Date(cur)
    dayEnd.setHours(24, 0, 0, 0)
    const effectiveEnd = dayEnd > b ? b : dayEnd
    const day = cur.getDay()
    if (day !== 0 && day !== 6) {
      total += (effectiveEnd - cur) / msPerHour
    }
    cur.setTime(dayEnd.getTime())
  }
  return total
}

function ProgramSummaryView({
  staffPerformance,
  filteredRuns,
  ruleDefinitions,
  ruleCategoryById,
  periodFilter,
  onPeriodChange,
  customStartDate,
  onCustomStartChange,
  customEndDate,
  onCustomEndChange,
  onSelectStaff,
  onSelectProgram,
}) {
  // 'staff' = staff ranked by errors; 'rules' = recurring rule failures; 'analytics' = deeper metrics (late notes to start).
  const [viewMode, setViewMode] = useState('staff')
  // Which analytic is selected under the Analytics view. More to come.
  const [analyticKey, setAnalyticKey] = useState('late-notes')
  // Set of category strings to include; empty Set = no filter (show everything).
  const [categoryFilter, setCategoryFilter] = useState(() => new Set())

  // Authoritative category list: pulled directly from the rule definitions
  // (same source the admin Validation Rules table uses). That way every chip
  // matches the `category` shown next to the rule, nothing is invented, and
  // renaming a category in the admin UI shows up here on reload.
  const availableCategories = useMemo(() => {
    const set = new Set()
    ruleDefinitions.forEach(r => {
      if (r?.category) set.add(r.category)
    })
    return Array.from(set).sort()
  }, [ruleDefinitions])

  // Resolve a validation-result rule's category from the authoritative
  // definition by rule_id; only falls back to any category on the result
  // itself if we somehow don't have the definition loaded.
  const categoryForRule = (r) => {
    if (r?.rule_id && ruleCategoryById.has(r.rule_id)) return ruleCategoryById.get(r.rule_id)
    return r?.category || null
  }

  // Group staff by program, sort each list by errors desc (tie-break by name),
  // and sort programs by total errors desc (most problematic first). Re-derive
  // per-staff error counts + per-program rule failures from the raw failed
  // documents so the category filter can be applied cleanly.
  const programs = useMemo(() => {
    const isIncluded = (category) =>
      categoryFilter.size === 0 || categoryFilter.has(category)

    const map = new Map()
    staffPerformance.forEach(s => {
      const key = s.program || 'Unknown'
      if (!map.has(key)) map.set(key, [])
      map.get(key).push(s)
    })

    const entries = Array.from(map.entries()).map(([program, staff]) => {
      // Per-staff filtered error count (category resolved via rule definitions).
      const staffWithFilteredCounts = staff.map(s => {
        let filteredErrorCount = 0
        s.failedDocuments?.forEach(doc => {
          doc.rules?.forEach(r => {
            if (r.status === 'FAIL' && isIncluded(categoryForRule(r))) filteredErrorCount += 1
          })
        })
        return { ...s, errorCount: filteredErrorCount }
      })

      const sorted = [...staffWithFilteredCounts].sort((a, b) =>
        (b.errorCount ?? 0) - (a.errorCount ?? 0) || a.name.localeCompare(b.name)
      )
      const totalErrors = sorted.reduce((sum, s) => sum + (s.errorCount ?? 0), 0)

      // Per-program rule_name -> count, restricted to the selected categories.
      const ruleCounts = new Map()
      staff.forEach(s => {
        s.failedDocuments?.forEach(doc => {
          doc.rules?.forEach(r => {
            if (r.status !== 'FAIL') return
            if (!isIncluded(categoryForRule(r))) return
            const name = r.rule_name || r.rule_id
            if (!name) return
            ruleCounts.set(name, (ruleCounts.get(name) || 0) + 1)
          })
        })
      })
      const ruleFailures = Array.from(ruleCounts.entries())
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name))

      return { program, staff: sorted, totalErrors, ruleFailures }
    })
    entries.sort((a, b) => b.totalErrors - a.totalErrors || a.program.localeCompare(b.program))
    return entries
  }, [staffPerformance, categoryFilter, ruleCategoryById])

  // Analytics: notes submitted more than 48 business hours after their
  // service date. "Submitted" is approximated by the run timestamp (when the
  // note was picked up in a validation run). Computed per program so each
  // card can surface its own late count + list.
  const LATE_THRESHOLD_HOURS = 48
  const lateNotesByProgram = useMemo(() => {
    const map = new Map()
    ;(filteredRuns || []).forEach(run => {
      const runTs = run.timestamp ? new Date(run.timestamp) : null
      if (!runTs || Number.isNaN(runTs.getTime())) return
      run.documents?.forEach(doc => {
        const program = doc.field_values?.program || 'Unknown'
        const rawDate = doc.field_values?.date
        if (!rawDate) return
        const serviceDate = new Date(rawDate)
        if (Number.isNaN(serviceDate.getTime())) return
        const hours = businessHoursBetween(serviceDate, runTs)
        if (hours == null) return

        if (!map.has(program)) {
          map.set(program, { program, totalNotes: 0, lateNotes: [] })
        }
        const bucket = map.get(program)
        bucket.totalNotes += 1
        if (hours > LATE_THRESHOLD_HOURS) {
          bucket.lateNotes.push({
            doc,
            runId: run.validation_run_id,
            runTimestamp: run.timestamp,
            businessHours: hours,
            daysLate: Math.floor((hours - LATE_THRESHOLD_HOURS) / 24) + 1,
          })
        }
      })
    })
    // Sort each program's late notes by how late they are, descending.
    map.forEach(b => { b.lateNotes.sort((a, b) => b.businessHours - a.businessHours) })
    return map
  }, [filteredRuns])

  if (staffPerformance.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center bg-white rounded-lg shadow">
        <p className="text-gray-500">No staff data available.</p>
      </div>
    )
  }

  const toggleCategory = (cat) => {
    setCategoryFilter(prev => {
      const next = new Set(prev)
      if (next.has(cat)) next.delete(cat)
      else next.add(cat)
      return next
    })
  }

  return (
    <div className="flex flex-col">
      <div className="mb-4">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
            <h1 className="text-2xl font-semibold text-gray-900">Program Summary</h1>
            <p className="text-sm text-gray-500">
              {viewMode === 'staff'
                ? 'Staff ranked by errors within each program. Click a name to open the risk profile.'
                : viewMode === 'rules'
                  ? 'Rule failures ranked by frequency within each program.'
                  : 'Operational analytics across each program.'}
            </p>
          </div>
          <div className="inline-flex rounded-lg border border-gray-200 bg-white p-0.5 shadow-sm">
            {[
              { value: 'staff', label: 'By Staff' },
              { value: 'rules', label: 'By Rule' },
              { value: 'analytics', label: 'Analytics' },
            ].map(opt => (
              <button
                key={opt.value}
                onClick={() => setViewMode(opt.value)}
                className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                  viewMode === opt.value
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>

        {/* Filters row: category pills on the left, compact date chip on the right */}
        <div className="mt-3 flex items-center gap-3 flex-wrap">
          {availableCategories.length > 0 && (
            <>
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
            </>
          )}

          <div className="ml-auto inline-flex items-center gap-1.5 bg-white border border-gray-200 rounded-full pl-2 pr-1 py-0.5 shadow-sm">
            <svg className="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
            </svg>
            <select
              value={periodFilter}
              onChange={(e) => onPeriodChange(e.target.value)}
              className="text-xs font-medium text-gray-700 bg-transparent border-0 focus:outline-none focus:ring-0 pr-1 py-0.5 cursor-pointer"
            >
              <option value="all">All time</option>
              <option value="24h">Last 24 hours</option>
              <option value="7d">Last 7 days</option>
              <option value="30d">Last 30 days</option>
              <option value="90d">Last 90 days</option>
              <option value="custom">Custom range</option>
            </select>
          </div>
        </div>

        {periodFilter === 'custom' && (
          <div className="mt-3 flex items-end gap-3 flex-wrap">
            <div className="flex flex-col">
              <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">From</label>
              <input
                type="date"
                value={customStartDate}
                onChange={(e) => onCustomStartChange(e.target.value)}
                className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div className="flex flex-col">
              <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">To</label>
              <input
                type="date"
                value={customEndDate}
                onChange={(e) => onCustomEndChange(e.target.value)}
                className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <button
              onClick={() => { onPeriodChange('all'); onCustomStartChange(''); onCustomEndChange('') }}
              className="text-sm text-blue-600 hover:text-blue-800 px-2 py-2"
            >
              Clear
            </button>
          </div>
        )}
      </div>

      {/*
        auto-fit + minmax makes the grid responsive: with few programs the cards
        stretch to fill the row; with many, they wrap at the min width. Bump
        the floor (420px) to keep each card visibly "wide".
      */}
      <div className="grid gap-5 grid-cols-[repeat(auto-fit,minmax(420px,1fr))]">
        {programs.map(p => (
          <ProgramSummaryCard
            key={p.program}
            program={p.program}
            staff={p.staff}
            totalErrors={p.totalErrors}
            ruleFailures={p.ruleFailures}
            viewMode={viewMode}
            analyticKey={analyticKey}
            onAnalyticChange={setAnalyticKey}
            lateNotesSummary={lateNotesByProgram.get(p.program)}
            onSelectStaff={onSelectStaff}
            onSelectProgram={onSelectProgram}
          />
        ))}
      </div>
    </div>
  )
}


function ProgramSummaryCard({
  program,
  staff,
  totalErrors,
  ruleFailures,
  viewMode,
  analyticKey,
  onAnalyticChange,
  lateNotesSummary,
  onSelectStaff,
  onSelectProgram,
}) {
  const isAnalytics = viewMode === 'analytics'
  return (
    <div className="bg-gray-50 rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-6 flex flex-col">
      <div className="mb-3 pb-3 border-b border-gray-200">
        <button
          onClick={() => onSelectProgram?.(program)}
          className="text-sm font-semibold text-blue-600 hover:text-blue-800 hover:underline uppercase tracking-wide truncate block text-left w-full"
          title={`Filter staff list by ${program}`}
        >
          {program}
        </button>
        <p className="text-sm text-gray-600 mt-1">
          {isAnalytics
            ? `${lateNotesSummary?.totalNotes ?? 0} notes in this program`
            : `${staff.length} staff · ${totalErrors} ${totalErrors === 1 ? 'error' : 'errors'}`}
        </p>
      </div>

      {isAnalytics ? (
        <AnalyticsBody
          analyticKey={analyticKey}
          onAnalyticChange={onAnalyticChange}
          lateNotesSummary={lateNotesSummary}
        />
      ) : (
        /* Fixed height for ~5 rows (each row ≈ 32px) before scrolling kicks in */
        <div className="overflow-y-auto max-h-40 -mx-2">
          {viewMode === 'rules' ? (
            ruleFailures.length === 0 ? (
              <p className="px-2 py-1.5 text-sm text-gray-400">No rule failures.</p>
            ) : (
              ruleFailures.map(rule => (
                <div
                  key={rule.name}
                  className="w-full flex items-center justify-between px-2 py-1.5 text-sm"
                >
                  <span className="text-gray-900 truncate" title={rule.name}>{rule.name}</span>
                  <span className="text-base font-semibold text-gray-700 tabular-nums flex-shrink-0 ml-2">
                    {rule.count}
                  </span>
                </div>
              ))
            )
          ) : (
            staff.map(s => (
              <button
                key={s.name}
                onClick={() => onSelectStaff(s)}
                className="w-full flex items-center justify-between px-2 py-1.5 rounded text-sm hover:bg-white transition-colors"
              >
                <span className="text-gray-900 truncate">{s.name}</span>
                <span className="text-base font-semibold text-gray-700 tabular-nums flex-shrink-0 ml-2">
                  {s.errorCount ?? 0}
                </span>
              </button>
            ))
          )}
        </div>
      )}
    </div>
  )
}

// Analytics body renders one of the available analytic lenses. For now only
// "late-notes" exists; the selector is already wired for future additions.
function AnalyticsBody({ analyticKey, onAnalyticChange, lateNotesSummary }) {
  const options = [
    { value: 'late-notes', label: 'Late notes' },
  ]
  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <span className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider">Metric</span>
        <select
          value={analyticKey}
          onChange={(e) => onAnalyticChange(e.target.value)}
          className="text-xs font-medium text-gray-700 bg-white border border-gray-200 rounded-full px-2.5 py-0.5 focus:outline-none focus:ring-2 focus:ring-blue-500 cursor-pointer"
        >
          {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
      </div>

      {analyticKey === 'late-notes' && (
        <LateNotesAnalytic summary={lateNotesSummary} />
      )}
    </div>
  )
}

function LateNotesAnalytic({ summary }) {
  const total = summary?.totalNotes ?? 0
  const late = summary?.lateNotes?.length ?? 0
  const pct = total > 0 ? Math.round((late / total) * 100) : 0

  if (total === 0) {
    return (
      <p className="text-sm text-gray-400 italic">No notes in this window.</p>
    )
  }

  const toneBar = pct >= 25 ? 'bg-red-500' : pct >= 10 ? 'bg-yellow-500' : 'bg-green-500'

  return (
    <div>
      {/* Headline */}
      <div className="flex items-baseline justify-between mb-1">
        <div className="flex items-baseline gap-1.5">
          <span className="text-xl font-bold text-gray-900 tabular-nums">{late}</span>
          <span className="text-xs text-gray-500">of {total} submitted late (&gt;48 business hrs)</span>
        </div>
        <span className="text-sm font-semibold tabular-nums text-gray-700">{pct}%</span>
      </div>
      <div className="w-full h-1.5 bg-gray-200 rounded-full overflow-hidden mb-3">
        <div className={`${toneBar} h-full rounded-full`} style={{ width: `${pct}%` }} />
      </div>

      {/* Late notes list */}
      {late === 0 ? (
        <p className="text-sm text-gray-400 italic">All notes submitted within 48 business hours.</p>
      ) : (
        <div className="overflow-y-auto max-h-40 -mx-2">
          {summary.lateNotes.map((entry, idx) => {
            const employee = entry.doc.field_values?.employee_name || 'Unknown'
            const date = entry.doc.field_values?.date || '—'
            return (
              <div
                key={`${entry.doc.document_id}-${idx}`}
                className="flex items-center justify-between px-2 py-1 text-xs"
              >
                <div className="min-w-0 flex items-baseline gap-1.5">
                  <span className="text-gray-900 truncate">{employee}</span>
                  <span className="text-gray-400">· {date}</span>
                </div>
                <span className="text-gray-700 tabular-nums flex-shrink-0 ml-2">
                  {entry.daysLate}d late
                </span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}


function FilterBar({
  periodFilter,
  onPeriodChange,
  customStartDate,
  onCustomStartChange,
  customEndDate,
  onCustomEndChange,
  allStaff,
  selectedStaffName,
  onSelectStaffName,
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 mb-6 flex flex-wrap items-end gap-3">
      <div className="flex flex-col">
        <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">Period</label>
        <select
          value={periodFilter}
          onChange={(e) => onPeriodChange(e.target.value)}
          className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">All time</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
          <option value="90d">Last 90 days</option>
          <option value="custom">Custom range</option>
        </select>
      </div>

      {periodFilter === 'custom' && (
        <>
          <div className="flex flex-col">
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">From</label>
            <input
              type="date"
              value={customStartDate}
              onChange={(e) => onCustomStartChange(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div className="flex flex-col">
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">To</label>
            <input
              type="date"
              value={customEndDate}
              onChange={(e) => onCustomEndChange(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </>
      )}

      <div className="flex flex-col flex-1 min-w-[200px]">
        <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">Staff name</label>
        <select
          value={selectedStaffName}
          onChange={(e) => onSelectStaffName(e.target.value)}
          className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {allStaff.map(s => (
            <option key={s.name} value={s.name}>{s.name}</option>
          ))}
        </select>
      </div>
    </div>
  )
}


function StaffListItem({ staff, selected, onClick }) {
  const getPassRateColor = (rate) => {
    if (rate == null) return 'bg-gray-300'
    if (rate === 100) return 'bg-green-500'
    if (rate >= 75) return 'bg-yellow-500'
    return 'bg-red-500'
  }

  const getPassRateBadgeStyle = (rate) => {
    if (rate == null) return 'bg-gray-100 text-gray-600'
    if (rate === 100) return 'bg-green-100 text-green-800'
    if (rate >= 75) return 'bg-yellow-100 text-yellow-800'
    return 'bg-red-100 text-red-800'
  }

  const getTrendIcon = (staff) => {
    // Simple trend calculation based on error count
    if (staff.errorCount > 10) return { icon: '↘', color: 'text-red-500' }
    if (staff.errorCount > 5) return { icon: '↗', color: 'text-yellow-500' }
    return { icon: '↗', color: 'text-green-500' }
  }

  const trend = getTrendIcon(staff)

  return (
    <div
      onClick={onClick}
      className={`px-4 py-3 border-b border-gray-100 cursor-pointer transition-colors ${
        selected ? 'bg-blue-50 border-l-4 border-l-blue-500' : 'hover:bg-gray-50'
      }`}
    >
      <div className="flex items-start justify-between mb-1">
        <div>
          <span className="text-sm font-medium text-gray-900">{staff.name}</span>
          <div className="text-xs text-gray-600 uppercase">{staff.program}</div>
        </div>
        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getPassRateBadgeStyle(staff.passRate)}`}>
          {staff.passRate == null ? '-' : `${staff.passRate}%`} Pass
        </span>
      </div>

      {/* Progress bar */}
      <div className="w-full h-1.5 bg-gray-200 rounded-full mt-2 mb-2">
        <div
          className={`h-1.5 rounded-full ${getPassRateColor(staff.passRate)}`}
          style={{ width: `${staff.passRate ?? 0}%` }}
        />
      </div>

      <div className="flex items-center justify-between text-xs">
        <span className="text-gray-500">{staff.errorCount} Errors</span>
        <span className={trend.color}>{trend.icon}</span>
      </div>
    </div>
  )
}


function StaffDetailPanel({ staff, filterBar, onBack }) {
  const activeBlockers = staff.failedDocuments.length
  const avgAuditScore = staff.passRate
  const primaryRisk = staff.recurringFailures[0]?.name || 'None'

  // Selected recurring-failure rule. When set, Flagged Notes below shows only
  // notes that actually failed this rule, and each card surfaces that rule's
  // reasoning (rather than just the first fail).
  const [selectedRuleName, setSelectedRuleName] = useState(null)

  // Reset the drill-down whenever we switch staff.
  useEffect(() => {
    setSelectedRuleName(null)
  }, [staff.name])

  const visibleFailedDocuments = selectedRuleName
    ? staff.failedDocuments.filter(doc =>
        doc.rules?.some(r =>
          (r.rule_name || r.rule_id) === selectedRuleName && r.status === 'FAIL'
        )
      )
    : staff.failedDocuments

  return (
    <div className="flex flex-col">
      {/* Header */}
      <div className="flex items-start justify-between mb-6">
        <div>
          {onBack && (
            <button
              onClick={onBack}
              className="text-sm text-blue-600 hover:text-blue-800 mb-2 inline-flex items-center gap-1"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
              Back to program summary
            </button>
          )}
          <h1 className="text-2xl font-semibold text-gray-900">{staff.name}</h1>
          <p className="text-sm text-gray-500">
            Detailed Risk Profile - {staff.program} Team
          </p>
        </div>
        <button className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors">
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          Schedule Coaching
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-white rounded-lg border border-gray-200 p-4 text-center">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-1">Active Blockers</div>
          <div className="text-3xl font-bold text-gray-900">{activeBlockers}</div>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 p-4 text-center">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-1">Avg. Audit Score</div>
          <div className="text-3xl font-bold text-gray-900">{avgAuditScore == null ? '-' : `${avgAuditScore}%`}</div>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 p-4 text-center">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-1">Primary Risk</div>
          <div className="text-xl font-bold text-gray-900 truncate">{primaryRisk}</div>
        </div>
      </div>

      {/* Filters */}
      {filterBar}

      {/* Recurring Rule Failures */}
      {staff.recurringFailures.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-4 mb-6">
          <h3 className="text-sm font-semibold text-red-600 uppercase tracking-wide mb-4">
            Recurring Rule Failures
          </h3>
          <div className="space-y-3">
            {staff.recurringFailures.map((failure, idx) => {
              const percentage = staff.failedDocuments.length > 0
                ? Math.round((failure.count / staff.failedDocuments.length) * 100)
                : 0
              const active = selectedRuleName === failure.name
              return (
                <button
                  key={idx}
                  onClick={() => setSelectedRuleName(active ? null : failure.name)}
                  aria-pressed={active}
                  className={`w-full flex items-center justify-between p-3 rounded-lg text-left transition-colors ${
                    active
                      ? 'bg-red-50 ring-2 ring-red-400'
                      : 'bg-gray-50 hover:bg-gray-100'
                  }`}
                  title={active ? 'Click to clear filter' : 'Click to filter Flagged Notes by this rule'}
                >
                  <div className="flex items-center gap-3">
                    <div className="w-8 h-8 bg-red-100 rounded-lg flex items-center justify-center flex-shrink-0">
                      <svg className="w-4 h-4 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                      </svg>
                    </div>
                    <div>
                      <div className="text-sm font-medium text-red-600">{failure.name}</div>
                      <div className="text-xs text-gray-500">Found in {percentage}% of failed notes.</div>
                    </div>
                  </div>
                  <span className={`px-3 py-1 rounded text-xs font-medium flex-shrink-0 ${
                    percentage >= 40 ? 'bg-red-600 text-white' : 'bg-yellow-100 text-yellow-800'
                  }`}>
                    {percentage >= 40 ? 'Critical Risk' : 'High Risk'}
                  </span>
                </button>
              )
            })}
          </div>
        </div>
      )}

      {/* Flagged Notes for Review */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center justify-between mb-4 gap-3 flex-wrap">
          <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide flex items-center gap-2">
            <svg className="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            Flagged Notes for Review ({visibleFailedDocuments.length}{selectedRuleName ? ` of ${staff.failedDocuments.length}` : ''})
          </h3>
          {selectedRuleName && (
            <div className="inline-flex items-center gap-1.5 bg-red-50 border border-red-200 rounded-full pl-3 pr-1 py-0.5">
              <span className="text-xs text-red-700 truncate max-w-[320px]" title={selectedRuleName}>
                Rule: {selectedRuleName}
              </span>
              <button
                onClick={() => setSelectedRuleName(null)}
                className="text-red-600 hover:text-red-800 rounded-full p-0.5"
                aria-label="Clear rule filter"
                title="Clear rule filter"
              >
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          )}
        </div>
        <div className="space-y-3">
          {visibleFailedDocuments.slice(0, 10).map((doc, idx) => {
            // When filtering by a recurring rule, prefer that rule's failure so
            // the reasoning matches what the user clicked; otherwise fall back
            // to the first failing rule on the doc.
            const failedRule = selectedRuleName
              ? doc.rules?.find(r =>
                  (r.rule_name || r.rule_id) === selectedRuleName && r.status === 'FAIL'
                ) || doc.rules?.find(r => r.status === 'FAIL')
              : doc.rules?.find(r => r.status === 'FAIL')
            const severity = doc.summary?.failed > 2 ? 'Blocker' : 'High'

            // Extract reasoning
            const reasoning = failedRule?.message?.replace(/^(FAIL|PASS|SKIP)\s*[-:]\s*/i, '') || 'No details available'

            return (
              <div key={idx} className="p-4 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors cursor-pointer">
                <div className="flex items-start justify-between">
                  <div className="flex items-start gap-3">
                    <div className="w-8 h-8 bg-gray-200 rounded-lg flex items-center justify-center mt-0.5">
                      <svg className="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                      </svg>
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <a
                          href={getCredibleLink(doc.document_id)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-sm font-medium text-blue-600 hover:text-blue-800 hover:underline"
                          onClick={(e) => e.stopPropagation()}
                        >
                          #{doc.document_id}
                        </a>
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                          severity === 'Blocker' ? 'bg-gray-800 text-white' : 'bg-gray-200 text-gray-700'
                        }`}>
                          {severity}
                        </span>
                      </div>
                      <div className="text-sm font-medium text-gray-700 mt-0.5">
                        {failedRule?.rule_name || 'Unknown Rule'}
                      </div>
                      <div className="text-xs text-gray-500 mt-0.5 line-clamp-1">
                        {reasoning}
                      </div>
                    </div>
                  </div>
                  <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                </div>
              </div>
            )
          })}
          {visibleFailedDocuments.length === 0 && (
            <div className="text-center py-8 text-gray-500">
              {selectedRuleName
                ? 'No notes failed this rule for this staff member.'
                : 'No flagged notes for this staff member.'}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
