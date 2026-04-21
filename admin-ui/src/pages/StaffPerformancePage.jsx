import { useState, useEffect, useMemo } from 'react'
import { useParams } from 'react-router-dom'
import { api } from '../api/client.js'

// Generate link to Credible BH for a document ID
const getCredibleLink = (documentId) =>
  `https://www.cbh3.crediblebh.com/visit/clientvisit_view.asp?clientvisit_id=${documentId}&provportal=0`

export function StaffPerformancePage() {
  const { orgId } = useParams()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [selectedStaff, setSelectedStaff] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [periodFilter, setPeriodFilter] = useState('all')
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')

  useEffect(() => {
    // Load all validation runs to aggregate staff performance
    api.listValidationRuns(orgId)
      .then(async (runsData) => {
        const runList = runsData.runs.slice(0, 10)
        // Load details for each run to get staff data
        const runsWithDetails = await Promise.all(
          runList.map(async run => ({
            ...(await api.getValidationRun(orgId, run.validation_run_id)),
            validation_run_id: run.validation_run_id,
            timestamp: run.timestamp,
          }))
        )
        setData(runsWithDetails)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  // Aggregate staff performance from all validation runs
  const staffPerformance = useMemo(() => {
    if (!data) return []

    const staffMap = new Map()

    const dayMs = 24 * 60 * 60 * 1000
    const now = Date.now()
    let startCutoff = null
    let endCutoff = null
    if (periodFilter === '7d') startCutoff = now - 7 * dayMs
    else if (periodFilter === '30d') startCutoff = now - 30 * dayMs
    else if (periodFilter === '90d') startCutoff = now - 90 * dayMs
    else if (periodFilter === 'custom') {
      if (customStartDate) startCutoff = new Date(customStartDate).getTime()
      // endCutoff is exclusive — include the full end day
      if (customEndDate) endCutoff = new Date(customEndDate).getTime() + dayMs
    }

    const filteredData = (startCutoff == null && endCutoff == null)
      ? data
      : data.filter(run => {
          if (!run.timestamp) return true
          const t = new Date(run.timestamp).getTime()
          if (startCutoff != null && t < startCutoff) return false
          if (endCutoff != null && t >= endCutoff) return false
          return true
        })

    filteredData.forEach(run => {
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
  }, [data, periodFilter, customStartDate, customEndDate])

  // Filter staff by search
  const filteredStaff = useMemo(() => {
    if (!searchTerm) return staffPerformance
    const search = searchTerm.toLowerCase()
    return staffPerformance.filter(s =>
      s.name.toLowerCase().includes(search) ||
      s.program.toLowerCase().includes(search)
    )
  }, [staffPerformance, searchTerm])

  // Auto-select first staff member, or refresh the selected staff's data when the filter changes
  useEffect(() => {
    if (filteredStaff.length === 0) return
    if (!selectedStaff) {
      setSelectedStaff(filteredStaff[0])
      return
    }
    const match = filteredStaff.find(s => s.name === selectedStaff.name)
    if (match && match !== selectedStaff) {
      setSelectedStaff(match)
    } else if (!match) {
      setSelectedStaff(filteredStaff[0])
    }
  }, [filteredStaff, selectedStaff])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-gray-500">Loading staff performance data...</p>
      </div>
    )
  }

  if (error) {
    return <div className="p-4"><p className="text-red-600">Error: {error}</p></div>
  }

  return (
    <div className="flex gap-6">
      {/* Left Panel - Staff Standings */}
      <div className="w-80 flex flex-col bg-white rounded-lg shadow sticky top-4 self-start max-h-[calc(100vh-100px)]">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold text-blue-600 uppercase tracking-wide">
              Staff Standings
            </h2>
            <span className="text-xs bg-gray-100 text-gray-600 px-2 py-1 rounded">
              {orgId}
            </span>
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

      {/* Right Panel - Staff Detail */}
      <div className="flex-1 flex flex-col">
        {selectedStaff ? (
          <StaffDetailPanel
            staff={selectedStaff}
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
          <div className="flex-1 flex items-center justify-center bg-white rounded-lg shadow">
            <p className="text-gray-500">Select a staff member to view details</p>
          </div>
        )}
      </div>
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
          <div className="text-xs text-red-600 uppercase">{staff.program}</div>
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


function StaffDetailPanel({ staff, filterBar }) {
  const activeBlockers = staff.failedDocuments.length
  const avgAuditScore = staff.passRate
  const primaryRisk = staff.recurringFailures[0]?.name || 'None'

  return (
    <div className="flex flex-col">
      {/* Header */}
      <div className="flex items-start justify-between mb-6">
        <div>
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
              return (
                <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    <div className="w-8 h-8 bg-red-100 rounded-lg flex items-center justify-center">
                      <svg className="w-4 h-4 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                      </svg>
                    </div>
                    <div>
                      <div className="text-sm font-medium text-red-600">{failure.name}</div>
                      <div className="text-xs text-gray-500">Found in {percentage}% of failed notes.</div>
                    </div>
                  </div>
                  <span className={`px-3 py-1 rounded text-xs font-medium ${
                    percentage >= 40 ? 'bg-red-600 text-white' : 'bg-yellow-100 text-yellow-800'
                  }`}>
                    {percentage >= 40 ? 'Critical Risk' : 'High Risk'}
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Flagged Notes for Review */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-4 flex items-center gap-2">
          <svg className="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          Flagged Notes for Review ({staff.failedDocuments.length})
        </h3>
        <div className="space-y-3">
          {staff.failedDocuments.slice(0, 10).map((doc, idx) => {
            const failedRule = doc.rules?.find(r => r.status === 'FAIL')
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
          {staff.failedDocuments.length === 0 && (
            <div className="text-center py-8 text-gray-500">
              No flagged notes for this staff member.
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
