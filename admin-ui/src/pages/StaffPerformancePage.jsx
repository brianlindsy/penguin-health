import { useState, useEffect, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
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
            orgId={orgId}
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


// Business-hours math. We count whole business days between two dates
// (normalizing both to start-of-day) and multiply by 24. That way the
// threshold aligns with how humans talk about it: a note dated 4/20 picked
// up by a run on 4/22 is "2 business days later" = 48 hours, which is NOT
// strictly greater than 48 — so the note isn't late. A 4/20 service date
// with a 4/23 run is 3 business days = 72 hours = late.
function businessHoursBetween(start, end) {
  const a = start instanceof Date ? new Date(start) : new Date(start)
  const b = end instanceof Date ? new Date(end) : new Date(end)
  if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return null
  a.setHours(0, 0, 0, 0)
  b.setHours(0, 0, 0, 0)
  if (b <= a) return 0
  let days = 0
  const cur = new Date(a)
  while (cur < b) {
    cur.setDate(cur.getDate() + 1)
    const day = cur.getDay()
    if (day !== 0 && day !== 6) days += 1
  }
  return days * 24
}

function ProgramSummaryView({
  orgId,
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

  // High-level KPIs shown at the top of the page. Sums/averages are taken
  // across the already-filtered staffPerformance so they respect the period
  // and category filters.
  const overview = useMemo(() => {
    const staffCount = staffPerformance.length
    let totalErrors = 0
    let auditedDocs = 0
    let passedDocs = 0
    let failedDocs = 0
    staffPerformance.forEach(s => {
      totalErrors += s.errorCount || 0
      auditedDocs += s.auditedDocumentCount || 0
      passedDocs += s.passedDocumentCount || 0
      failedDocs += s.failedDocuments?.length || 0
    })
    const avgPassRate = auditedDocs > 0 ? Math.round((passedDocs / auditedDocs) * 100) : null

    let totalLateNotes = 0
    let totalNotes = 0
    lateNotesByProgram.forEach(b => {
      totalLateNotes += b.lateNotes?.length || 0
      totalNotes += b.totalNotes || 0
    })

    return { staffCount, totalErrors, failedDocs, avgPassRate, totalLateNotes, totalNotes }
  }, [staffPerformance, lateNotesByProgram])

  // Top failing rules across everyone currently in scope — feeds the overview chart.
  const topFailingRules = useMemo(() => {
    const map = new Map()
    staffPerformance.forEach(s => {
      s.ruleFailures?.forEach((count, name) => {
        if (!name) return
        map.set(name, (map.get(name) || 0) + count)
      })
    })
    return Array.from(map.entries())
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name))
      .slice(0, 8)
  }, [staffPerformance])

  return (
    <div className="flex flex-col">
      {/* Hero: title + KPI stats + top-rules chart */}
      <div className="mb-5">
        <h1 className="text-2xl font-semibold text-gray-900">Program Summary</h1>
        <p className="text-sm text-gray-500 mt-1">
          A system-level view of staff performance, rule failures, and documentation timeliness.
        </p>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[auto,1fr] gap-4 mb-5">
        {/* KPI stat cards — stacked 2×2 on the left */}
        <div className="grid grid-cols-2 gap-3 xl:w-[320px]">
          <KpiCard
            label="Staff audited"
            value={overview.staffCount}
          />
          <KpiCard
            label="Total errors"
            value={overview.totalErrors}
            tone="red"
          />
          <KpiCard
            label="Avg pass rate"
            value={overview.avgPassRate == null ? '—' : `${overview.avgPassRate}%`}
            tone={overview.avgPassRate == null ? null
              : overview.avgPassRate === 100 ? 'green'
              : overview.avgPassRate >= 75 ? 'amber'
              : 'red'}
          />
          <KpiCard
            label="Late notes"
            value={overview.totalLateNotes}
            subtext={overview.totalNotes > 0 ? `of ${overview.totalNotes}` : null}
            tone={overview.totalLateNotes === 0 ? null : 'amber'}
          />
        </div>

        {/* Top failing rules chart */}
        <TopRulesChart rules={topFailingRules} />
      </div>

      <div className="mb-4">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
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

      {viewMode === 'analytics' && analyticKey === 'late-notes' ? (
        <LateNotesAnalyticsView
          orgId={orgId}
          programs={programs}
          lateNotesByProgram={lateNotesByProgram}
        />
      ) : (
        /*
          auto-fit + minmax makes the grid responsive: with few programs the cards
          stretch to fill the row; with many, they wrap at the min width. Bump
          the floor (420px) to keep each card visibly "wide".
        */
        <div className="grid gap-5 grid-cols-[repeat(auto-fit,minmax(420px,1fr))]">
          {programs.map(p => (
            <ProgramSummaryCard
              key={p.program}
              program={p.program}
              staff={p.staff}
              totalErrors={p.totalErrors}
              ruleFailures={p.ruleFailures}
              viewMode={viewMode}
              onSelectStaff={onSelectStaff}
              onSelectProgram={onSelectProgram}
            />
          ))}
        </div>
      )}
    </div>
  )
}


function ProgramSummaryCard({
  program,
  staff,
  totalErrors,
  ruleFailures,
  viewMode,
  onSelectStaff,
  onSelectProgram,
}) {
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
          {staff.length} staff · {totalErrors} {totalErrors === 1 ? 'error' : 'errors'}
        </p>
      </div>

      {/* Fixed height for ~5 rows (each row ≈ 32px) before scrolling kicks in */}
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
    </div>
  )
}

// Analytics overview: polished bar chart across programs + a stack of
// collapsible program boxes showing each program's late notes (linked).
// Compact top-of-page KPI. Tone tints the value color to match the metric
// (errors red, perfect-pass green, warnings amber).
function KpiCard({ label, value, subtext, tone }) {
  const valueTone = tone === 'red' ? 'text-red-600'
    : tone === 'green' ? 'text-emerald-600'
    : tone === 'amber' ? 'text-amber-600'
    : 'text-gray-900'
  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm px-4 py-3">
      <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider">{label}</div>
      <div className={`text-2xl font-bold ${valueTone} mt-1 tabular-nums leading-tight`}>
        {value}
      </div>
      {subtext && <div className="text-[10px] text-gray-400 mt-0.5">{subtext}</div>}
    </div>
  )
}

// Horizontal bar chart for the "what's failing most" overview up top. Simple
// and scannable, so the user can spot the worst offenders without switching
// views.
function TopRulesChart({ rules }) {
  const max = Math.max(...rules.map(r => r.count), 1)
  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
      <div className="flex items-baseline justify-between mb-3 gap-4 flex-wrap">
        <div>
          <h2 className="text-sm font-semibold text-gray-900">Top failing rules</h2>
          <p className="text-[11px] text-gray-500 mt-0.5">
            Ranked by failure count across staff in the selected window.
          </p>
        </div>
        <span className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider">Failures</span>
      </div>
      {rules.length === 0 ? (
        <p className="text-sm text-gray-400 italic py-4">No rule failures in the selected window.</p>
      ) : (
        <div className="space-y-2">
          {rules.map(rule => {
            const pct = (rule.count / max) * 100
            return (
              <div
                key={rule.name}
                className="grid grid-cols-[minmax(0,14rem),1fr,2.5rem] items-center gap-3"
              >
                <div className="text-xs text-gray-700 truncate" title={rule.name}>
                  {rule.name}
                </div>
                <div className="relative h-4 bg-gray-100 rounded">
                  <div
                    className="h-full rounded bg-red-500 transition-[width] duration-500 ease-out"
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <div className="text-xs font-semibold text-gray-900 tabular-nums text-right">
                  {rule.count}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}


function LateNotesAnalyticsView({ orgId, programs, lateNotesByProgram }) {
  const entries = programs.map(p => {
    const s = lateNotesByProgram.get(p.program)
    return {
      program: p.program,
      late: s?.lateNotes?.length ?? 0,
      total: s?.totalNotes ?? 0,
      lateNotes: s?.lateNotes ?? [],
    }
  }).filter(e => e.total > 0)

  const sorted = [...entries].sort((a, b) => b.late - a.late || a.program.localeCompare(b.program))

  if (sorted.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-8 text-center text-gray-500">
        No notes in the selected window.
      </div>
    )
  }

  return (
    <div className="space-y-5">
      <LateNotesChart entries={sorted} />

      <div className="space-y-2">
        {sorted.map(e => (
          <LateNotesProgramBox key={e.program} orgId={orgId} entry={e} />
        ))}
      </div>
    </div>
  )
}

// Vertical bar chart: gridded plot area with y-axis tick labels on the
// left, column bars scaled to a "nice" y-max, and program name + count
// labels under each column. Intentionally reads as a proper chart rather
// than an infographic.
function LateNotesChart({ entries }) {
  const maxLate = Math.max(...entries.map(e => e.late), 1)
  const yMax = niceCeiling(maxLate)
  const ticks = [0, yMax * 0.25, yMax * 0.5, yMax * 0.75, yMax].map(t => Math.round(t))

  const totalLate = entries.reduce((sum, e) => sum + e.late, 0)
  const totalNotes = entries.reduce((sum, e) => sum + e.total, 0)
  const overallPct = totalNotes > 0 ? Math.round((totalLate / totalNotes) * 100) : 0

  const CHART_HEIGHT = 240 // px — fixed plot height so the chart reads consistently

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
      <div className="flex items-start justify-between gap-4 flex-wrap mb-5">
        <div>
          <h2 className="text-base font-semibold text-gray-900">Late notes by program</h2>
          <p className="text-xs text-gray-500 mt-0.5">
            Notes submitted &gt; 48 business hours after the service date.
          </p>
        </div>
        <div className="text-right">
          <div className="text-3xl font-bold text-gray-900 tabular-nums leading-none">
            {totalLate}
            <span className="text-base font-normal text-gray-400 ml-1">/ {totalNotes}</span>
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {overallPct}% overall late
          </div>
        </div>
      </div>

      {/* Chart body: y-axis on the left + plot area on the right */}
      <div className="flex">
        {/* Y-axis labels */}
        <div
          className="flex flex-col justify-between pr-2 text-[10px] text-gray-400 tabular-nums"
          style={{ height: CHART_HEIGHT }}
        >
          {[...ticks].reverse().map((t, i) => (
            <span key={i} className="leading-none text-right">{t}</span>
          ))}
        </div>

        {/* Plot area */}
        <div className="flex-1">
          <div
            className="relative border-l border-b border-gray-200"
            style={{ height: CHART_HEIGHT }}
          >
            {/* Horizontal gridlines */}
            {ticks.map((t, i) => {
              const bottom = yMax > 0 ? (t / yMax) * 100 : 0
              return (
                <div
                  key={i}
                  className="absolute left-0 right-0 border-t border-dashed border-gray-100"
                  style={{ bottom: `${bottom}%` }}
                />
              )
            })}

            {/* Bars */}
            <div className="absolute inset-0 flex items-end justify-around gap-3 px-3">
              {entries.map(e => {
                const latePct = e.total > 0 ? Math.round((e.late / e.total) * 100) : 0
                const heightPct = yMax > 0 ? (e.late / yMax) * 100 : 0
                const barColor = latePct >= 25 ? 'bg-red-500'
                  : latePct >= 10 ? 'bg-amber-500'
                  : 'bg-emerald-500'
                const barHover = latePct >= 25 ? 'hover:bg-red-600'
                  : latePct >= 10 ? 'hover:bg-amber-600'
                  : 'hover:bg-emerald-600'
                return (
                  <div
                    key={e.program}
                    className="flex-1 min-w-0 h-full flex flex-col items-center justify-end"
                    title={`${e.program}: ${e.late} late of ${e.total} (${latePct}%)`}
                  >
                    {/* Count label above bar */}
                    {e.late > 0 && (
                      <span
                        className="text-xs font-semibold text-gray-700 tabular-nums mb-1"
                        style={{ lineHeight: 1 }}
                      >
                        {e.late}
                      </span>
                    )}
                    {/* Bar */}
                    <div
                      className={`w-full max-w-[72px] rounded-t-md ${barColor} ${barHover} transition-all duration-500 ease-out`}
                      style={{
                        height: `${Math.max(heightPct, e.late > 0 ? 1 : 0)}%`,
                        minHeight: e.late > 0 ? 2 : 0,
                      }}
                    />
                  </div>
                )
              })}
            </div>
          </div>

          {/* X-axis labels */}
          <div className="flex justify-around gap-3 px-3 mt-2">
            {entries.map(e => (
              <div
                key={e.program}
                className="flex-1 min-w-0 text-center text-[11px] text-gray-600 truncate"
                title={e.program}
              >
                {e.program}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Legend */}
      <div className="flex items-center justify-end gap-4 mt-4 text-[11px] text-gray-500">
        <LegendSwatch className="bg-emerald-500" label="< 10% late" />
        <LegendSwatch className="bg-amber-500" label="10–25% late" />
        <LegendSwatch className="bg-red-500" label="≥ 25% late" />
      </div>
    </div>
  )
}

function LegendSwatch({ className, label }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className={`w-2.5 h-2.5 rounded-sm ${className}`} />
      {label}
    </span>
  )
}

// Round a number up to a "nice" axis value so the y-axis reads cleanly
// (whole-number ticks at 1 / 2 / 5 × 10^n rather than 7, 13, 34...).
function niceCeiling(n) {
  if (n <= 0) return 5
  const magnitude = Math.pow(10, Math.floor(Math.log10(n)))
  const normalized = n / magnitude
  let nice
  if (normalized <= 1) nice = 1
  else if (normalized <= 2) nice = 2
  else if (normalized <= 5) nice = 5
  else nice = 10
  return Math.max(nice * magnitude, 4) // ensure at least 4 so ticks render as distinct integers
}

// One program's expandable summary card: header shows the headline counts;
// clicking toggles a list of the late notes, each linking out to the note
// in Credible and to the validation run.
function LateNotesProgramBox({ orgId, entry }) {
  const [open, setOpen] = useState(false)
  const { program, late, total, lateNotes } = entry
  const pct = total > 0 ? Math.round((late / total) * 100) : 0
  const tone = pct >= 25 ? 'bg-red-500' : pct >= 10 ? 'bg-amber-500' : 'bg-emerald-500'
  const pctTextTone = pct >= 25 ? 'text-red-700' : pct >= 10 ? 'text-amber-700' : 'text-emerald-700'
  const canExpand = late > 0

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
      <button
        onClick={() => canExpand && setOpen(!open)}
        className={`w-full flex items-center gap-4 px-5 py-4 text-left transition-colors ${
          canExpand ? 'hover:bg-gray-50' : 'cursor-default'
        }`}
        aria-expanded={open}
        disabled={!canExpand}
      >
        <svg
          className={`w-4 h-4 flex-shrink-0 transition-transform ${open ? 'rotate-90' : ''} ${
            canExpand ? 'text-gray-400' : 'text-transparent'
          }`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>

        <div className="flex-1 min-w-0">
          <div className="flex items-baseline gap-2">
            <span className="text-sm font-semibold text-gray-900 truncate">{program}</span>
            <span className={`text-xs font-semibold ${pctTextTone}`}>{pct}%</span>
          </div>
          <div className="mt-1.5 relative h-1.5 bg-gray-100 rounded-full overflow-hidden max-w-md">
            <div className={`${tone} h-full rounded-full`} style={{ width: `${pct}%` }} />
          </div>
        </div>

        <div className="text-right flex-shrink-0">
          <div className="text-sm tabular-nums">
            <span className="font-bold text-gray-900">{late}</span>
            <span className="text-gray-400"> / {total}</span>
          </div>
          <div className="text-[10px] uppercase tracking-wide text-gray-400">late / total</div>
        </div>
      </button>

      {open && canExpand && (
        <div className="border-t border-gray-100 px-5 py-3 bg-gray-50">
          <ul className="divide-y divide-gray-100">
            {lateNotes.map((entry, idx) => (
              <LateNoteRow key={`${entry.doc.document_id}-${idx}`} orgId={orgId} entry={entry} />
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}

function LateNoteRow({ orgId, entry }) {
  const employee = entry.doc.field_values?.employee_name || 'Unknown'
  const date = entry.doc.field_values?.date || '—'
  const program = entry.doc.field_values?.program
  return (
    <li className="py-2 flex items-center justify-between gap-3 text-xs">
      <div className="min-w-0 flex items-center gap-2 flex-wrap">
        <span className="font-mono text-gray-500 flex-shrink-0">
          #{entry.doc.document_id}
        </span>
        <span className="text-gray-900 font-medium truncate">{employee}</span>
        {program && <span className="text-gray-400">· {program}</span>}
        <span className="text-gray-400">· service {date}</span>
      </div>
      <div className="flex items-center gap-3 flex-shrink-0">
        <span className="text-gray-700 tabular-nums font-semibold">
          {entry.daysLate}d late
        </span>
        {entry.runId && (
          <Link
            to={`/organizations/${orgId}/validation-runs/${entry.runId}`}
            className="text-blue-600 hover:text-blue-800 hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            View run
          </Link>
        )}
      </div>
    </li>
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
