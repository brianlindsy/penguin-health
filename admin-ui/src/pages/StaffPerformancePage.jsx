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

  useEffect(() => {
    // Load all validation runs to aggregate staff performance
    api.listValidationRuns(orgId)
      .then(async (runsData) => {
        // Load details for each run to get staff data
        const runsWithDetails = await Promise.all(
          runsData.runs.slice(0, 10).map(run =>
            api.getValidationRun(orgId, run.validation_run_id)
          )
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

    data.forEach(run => {
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
            ruleFailures: new Map(), // Track which rules fail most often
          })
        }

        const staff = staffMap.get(employeeName)
        staff.totalRules += doc.summary?.total_rules || 0
        staff.passedRules += doc.summary?.passed || 0
        staff.failedRules += doc.summary?.failed || 0
        staff.skippedRules += doc.summary?.skipped || 0
        staff.documents.push(doc)

        if (doc.summary?.failed > 0) {
          staff.failedDocuments.push(doc)
          // Track rule failures
          doc.rules?.forEach(rule => {
            if (rule.status === 'FAIL') {
              const count = staff.ruleFailures.get(rule.rule_name) || 0
              staff.ruleFailures.set(rule.rule_name, count + 1)
            }
          })
        }
      })
    })

    // Convert to array and calculate pass rates
    return Array.from(staffMap.values())
      .map(staff => ({
        ...staff,
        passRate: staff.totalRules > 0
          ? Math.round((staff.passedRules / staff.totalRules) * 100)
          : 0,
        errorCount: staff.failedRules,
        // Get top recurring failures
        recurringFailures: Array.from(staff.ruleFailures.entries())
          .sort((a, b) => b[1] - a[1])
          .slice(0, 5)
          .map(([name, count]) => ({ name, count })),
      }))
      .sort((a, b) => a.passRate - b.passRate) // Sort by pass rate ascending (worst first)
  }, [data])

  // Filter staff by search
  const filteredStaff = useMemo(() => {
    if (!searchTerm) return staffPerformance
    const search = searchTerm.toLowerCase()
    return staffPerformance.filter(s =>
      s.name.toLowerCase().includes(search) ||
      s.program.toLowerCase().includes(search)
    )
  }, [staffPerformance, searchTerm])

  // Auto-select first staff member
  useEffect(() => {
    if (filteredStaff.length > 0 && !selectedStaff) {
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
    <div className="flex gap-6 h-[calc(100vh-180px)]">
      {/* Left Panel - Staff Standings */}
      <div className="w-80 flex flex-col bg-white rounded-lg shadow">
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
          <StaffDetailPanel staff={selectedStaff} />
        ) : (
          <div className="flex-1 flex items-center justify-center bg-white rounded-lg shadow">
            <p className="text-gray-500">Select a staff member to view details</p>
          </div>
        )}
      </div>
    </div>
  )
}


function StaffListItem({ staff, selected, onClick }) {
  const getPassRateColor = (rate) => {
    if (rate >= 90) return 'bg-green-500'
    if (rate >= 75) return 'bg-yellow-500'
    return 'bg-red-500'
  }

  const getPassRateBadgeStyle = (rate) => {
    if (rate >= 90) return 'bg-green-100 text-green-800'
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
          {staff.passRate}% Pass
        </span>
      </div>

      {/* Progress bar */}
      <div className="w-full h-1.5 bg-gray-200 rounded-full mt-2 mb-2">
        <div
          className={`h-1.5 rounded-full ${getPassRateColor(staff.passRate)}`}
          style={{ width: `${staff.passRate}%` }}
        />
      </div>

      <div className="flex items-center justify-between text-xs">
        <span className="text-gray-500">{staff.errorCount} Errors</span>
        <span className={trend.color}>{trend.icon}</span>
      </div>
    </div>
  )
}


function StaffDetailPanel({ staff }) {
  const activeBlockers = staff.failedDocuments.length
  const avgAuditScore = staff.passRate
  const primaryRisk = staff.recurringFailures[0]?.name || 'None'

  return (
    <div className="flex flex-col h-full">
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
          <div className="text-3xl font-bold text-gray-900">{avgAuditScore}%</div>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 p-4 text-center">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-1">Primary Risk</div>
          <div className="text-xl font-bold text-gray-900 truncate">{primaryRisk}</div>
        </div>
      </div>

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
      <div className="bg-white rounded-lg border border-gray-200 p-4 flex-1 overflow-hidden flex flex-col">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-4 flex items-center gap-2">
          <svg className="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          Flagged Notes for Review ({staff.failedDocuments.length})
        </h3>
        <div className="flex-1 overflow-y-auto space-y-3">
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
