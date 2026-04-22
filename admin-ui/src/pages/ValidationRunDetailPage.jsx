import { useState, useEffect, useMemo } from 'react'
import { useParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'

// Field display labels for different organizations
const FIELD_LABELS = {
  service_id: 'Service ID',
  date: 'Service Date',
  program: 'Program',
  service_type: 'Service Type',
  diagnosis_code: 'Diagnosis Code',
  cpt_code: 'CPT Code',
  rate: 'Rate',
  employee_name: 'Employee',
  document_id: 'Document ID',
}

// Generate link to Credible BH for a document ID
const getCredibleLink = (documentId) =>
  `https://www.cbh3.crediblebh.com/visit/clientvisit_view.asp?clientvisit_id=${documentId}&provportal=0`

export function ValidationRunDetailPage() {
  const { orgId, runId } = useParams()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [selectedDoc, setSelectedDoc] = useState(null)
  const [selectedRule, setSelectedRule] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [ruleFilter, setRuleFilter] = useState('all')
  const [statusFilter, setStatusFilter] = useState('all') // 'all' | 'needs_action' | 'confirmed'
  const [programFilter, setProgramFilter] = useState('all')
  const [categoryFilter, setCategoryFilter] = useState('all')
  const [dateFilter, setDateFilter] = useState('all')
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')

  useEffect(() => {
    api.getValidationRun(orgId, runId)
      .then(result => {
        setData(result)
        // Auto-select first document with failures
        const firstFailed = result.documents?.find(d => d.summary?.failed > 0)
        if (firstFailed) {
          setSelectedDoc(firstFailed)
          const firstFailedRule = firstFailed.rules?.find(r => r.status === 'FAIL')
          if (firstFailedRule) setSelectedRule(firstFailedRule)
        } else if (result.documents?.length > 0) {
          setSelectedDoc(result.documents[0])
          if (result.documents[0].rules?.length > 0) {
            setSelectedRule(result.documents[0].rules[0])
          }
        }
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId, runId])

  // Compute summary stats
  const stats = useMemo(() => {
    if (!data?.documents) return { needsAction: 0, opportunities: 0, confirmed: 0, revenueAtRisk: 0 }

    let needsAction = 0
    let opportunities = 0
    let confirmed = 0
    let revenueAtRisk = 0

    data.documents.forEach(doc => {
      if (doc.summary?.failed > 0) {
        needsAction++
        // Sum up rate for documents with failures
        const rate = parseFloat(doc.field_values?.rate) || 0
        revenueAtRisk += rate
      } else {
        // No failures — confirmed. Skips are acceptable and still count here.
        confirmed++
      }
    })

    return { needsAction, opportunities, confirmed, revenueAtRisk }
  }, [data])

  // Get unique programs, categories, and rules for filters
  const { programs, categories, rules } = useMemo(() => {
    if (!data?.documents) return { programs: [], categories: [], rules: [] }

    const programSet = new Set()
    const categorySet = new Set()
    const ruleSet = new Set()

    data.documents.forEach(doc => {
      const program = doc.field_values?.program
      if (program) programSet.add(program)

      doc.rules?.forEach(rule => {
        if (rule.category) categorySet.add(rule.category)
        const name = rule.rule_name || rule.rule_id
        if (name) ruleSet.add(name)
      })
    })

    return {
      programs: Array.from(programSet).sort(),
      categories: Array.from(categorySet).sort(),
      rules: Array.from(ruleSet).sort(),
    }
  }, [data])

  // Filter documents
  const filteredDocs = useMemo(() => {
    if (!data?.documents) return []

    // Resolve date-filter cutoffs once per memo run. A doc's date comes from
    // field_values.date (service date, e.g. "04/20/2026"); when absent we
    // fall back to the run-level timestamp so the doc still participates.
    const dayMs = 24 * 60 * 60 * 1000
    const now = Date.now()
    let startCutoff = null
    let endCutoff = null
    if (dateFilter === '24h') startCutoff = now - dayMs
    else if (dateFilter === '7d') startCutoff = now - 7 * dayMs
    else if (dateFilter === '30d') startCutoff = now - 30 * dayMs
    else if (dateFilter === '90d') startCutoff = now - 90 * dayMs
    else if (dateFilter === 'custom') {
      if (customStartDate) startCutoff = new Date(customStartDate).getTime()
      if (customEndDate) endCutoff = new Date(customEndDate).getTime() + dayMs // end-inclusive
    }
    const runTimestampMs = data?.timestamp ? new Date(data.timestamp).getTime() : null

    return data.documents.filter(doc => {
      // Search filter
      if (searchTerm) {
        const search = searchTerm.toLowerCase()
        const matchesId = doc.document_id?.toLowerCase().includes(search)
        const matchesEmployee = doc.field_values?.employee_name?.toLowerCase().includes(search)
        const matchesProgram = doc.field_values?.program?.toLowerCase().includes(search)
        if (!matchesId && !matchesEmployee && !matchesProgram) return false
      }

      // Status filter (from the Needs Action / Confirmed summary cards at top)
      if (statusFilter === 'needs_action' && !(doc.summary?.failed > 0)) return false
      if (statusFilter === 'confirmed' && !(doc.summary?.failed === 0)) return false

      // Rule filter: keep docs where the selected rule actually FAILED. This
      // composes naturally with the "Needs Action" status filter — the user
      // can click Needs Action, pick a rule, and get the docs that failed
      // that specific rule.
      if (ruleFilter !== 'all') {
        const hasFailedRule = doc.rules?.some(r =>
          (r.rule_name || r.rule_id) === ruleFilter && r.status === 'FAIL'
        )
        if (!hasFailedRule) return false
      }

      // Program filter
      if (programFilter !== 'all' && doc.field_values?.program !== programFilter) return false

      // Category filter (document has at least one rule in category)
      if (categoryFilter !== 'all') {
        const hasCategory = doc.rules?.some(r => r.category === categoryFilter)
        if (!hasCategory) return false
      }

      // Date filter (uses the doc's service date; falls back to the run timestamp).
      if (startCutoff != null || endCutoff != null) {
        let t = null
        const rawDate = doc.field_values?.date
        if (rawDate) {
          const parsed = new Date(rawDate).getTime()
          if (!Number.isNaN(parsed)) t = parsed
        }
        if (t == null) t = runTimestampMs
        if (t == null) return true // no date info — don't exclude
        if (startCutoff != null && t < startCutoff) return false
        if (endCutoff != null && t >= endCutoff) return false
      }

      return true
    })
  }, [data, searchTerm, statusFilter, ruleFilter, programFilter, categoryFilter, dateFilter, customStartDate, customEndDate])

  if (loading) return <OrgWorkspaceLayout><div className="flex items-center justify-center h-64"><p className="text-gray-500">Loading validation run...</p></div></OrgWorkspaceLayout>
  if (error) return <OrgWorkspaceLayout><div className="p-4"><p className="text-red-600">Error: {error}</p></div></OrgWorkspaceLayout>
  if (!data) return <OrgWorkspaceLayout><div className="p-4"><p className="text-gray-500">Validation run not found</p></div></OrgWorkspaceLayout>

  return (
    <OrgWorkspaceLayout>
    <div className="h-full flex flex-col">
      {/* Summary Cards */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <SummaryCard
          label="NEEDS ACTION"
          value={stats.needsAction}
          color="red"
          active={statusFilter === 'needs_action'}
          onClick={() => setStatusFilter(statusFilter === 'needs_action' ? 'all' : 'needs_action')}
        />
        <SummaryCard
          label="CONFIRMED"
          value={stats.confirmed}
          color="green"
          active={statusFilter === 'confirmed'}
          onClick={() => setStatusFilter(statusFilter === 'confirmed' ? 'all' : 'confirmed')}
        />
        <SummaryCard
          label="REVENUE AT RISK"
          value={`$${stats.revenueAtRisk.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
          subtext={`${stats.needsAction} blocked claim${stats.needsAction !== 1 ? 's' : ''}`}
          color="blue"
          onClick={() => {}}
        />
      </div>

      {/* Search and Filters */}
      <div className="flex items-center gap-3 mb-4">
        <div className="flex-1 relative">
          <input
            type="text"
            placeholder="Search by ID, employee, or program..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-9 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
          <svg className="absolute left-3 top-2.5 h-4 w-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>

        <select
          value={ruleFilter}
          onChange={(e) => setRuleFilter(e.target.value)}
          className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 max-w-[240px] truncate"
        >
          <option value="all">All Rules</option>
          {rules.map(r => <option key={r} value={r}>{r}</option>)}
        </select>

        <select
          value={programFilter}
          onChange={(e) => setProgramFilter(e.target.value)}
          className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">All Programs</option>
          {programs.map(p => <option key={p} value={p}>{p}</option>)}
        </select>

        <select
          value={categoryFilter}
          onChange={(e) => setCategoryFilter(e.target.value)}
          className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">All Categories</option>
          {categories.map(c => <option key={c} value={c}>{c}</option>)}
        </select>

        <select
          value={dateFilter}
          onChange={(e) => setDateFilter(e.target.value)}
          className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">All Dates</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
          <option value="90d">Last 90 days</option>
          <option value="custom">Custom range</option>
        </select>
      </div>

      {dateFilter === 'custom' && (
        <div className="flex items-end gap-3 mb-4">
          <div className="flex flex-col">
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">From</label>
            <input
              type="date"
              value={customStartDate}
              onChange={(e) => setCustomStartDate(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div className="flex flex-col">
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">To</label>
            <input
              type="date"
              value={customEndDate}
              onChange={(e) => setCustomEndDate(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <button
            onClick={() => { setDateFilter('all'); setCustomStartDate(''); setCustomEndDate('') }}
            className="text-sm text-blue-600 hover:text-blue-800 px-2 py-2"
          >
            Clear
          </button>
        </div>
      )}

      {/* Split Panel */}
      <div className="flex-1 flex gap-4 min-h-0">
        {/* Left Panel - Document List */}
        <div className="w-1/3 bg-white rounded-lg shadow overflow-hidden flex flex-col">
          <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between">
            <span className="text-sm font-medium text-gray-700">
              {filteredDocs.length} Document{filteredDocs.length !== 1 ? 's' : ''}
            </span>
          </div>
          <div className="flex-1 overflow-y-auto max-h-[calc(5*9rem)]">
            {filteredDocs.map(doc => (
              <DocumentListItem
                key={doc.document_id}
                doc={doc}
                selected={selectedDoc?.document_id === doc.document_id}
                onClick={() => {
                  setSelectedDoc(doc)
                  // If the user is drilling into a specific rule, surface that
                  // rule's failure in the right panel; else show the first fail.
                  const ruleMatch = ruleFilter !== 'all'
                    ? doc.rules?.find(r =>
                        (r.rule_name || r.rule_id) === ruleFilter && r.status === 'FAIL'
                      )
                    : null
                  const firstFailedRule = doc.rules?.find(r => r.status === 'FAIL')
                  setSelectedRule(ruleMatch || firstFailedRule || doc.rules?.[0] || null)
                }}
              />
            ))}
            {filteredDocs.length === 0 && (
              <p className="p-4 text-sm text-gray-500">No documents match your filters.</p>
            )}
          </div>
        </div>

        {/* Right Panel - Detail View */}
        <div className="flex-1 bg-white rounded-lg shadow overflow-hidden flex flex-col">
          {selectedDoc ? (
            <DocumentDetailPanel
              doc={selectedDoc}
              selectedRule={selectedRule}
              onSelectRule={setSelectedRule}
            />
          ) : (
            <div className="flex-1 flex items-center justify-center text-gray-500">
              Select a document to view details
            </div>
          )}
        </div>
      </div>
    </div>
    </OrgWorkspaceLayout>
  )
}


function SummaryCard({ label, value, subtext, color, active, onClick }) {
  // Active = soft transparent tint, matching the card's accent color.
  const activeStyles = {
    red: 'border-red-300 bg-red-500/10',
    yellow: 'border-yellow-300 bg-yellow-500/10',
    green: 'border-green-300 bg-green-500/10',
    blue: 'border-blue-300 bg-blue-500/10',
  }

  const textStyles = {
    red: 'text-red-700',
    yellow: 'text-yellow-700',
    green: 'text-green-700',
    blue: 'text-blue-700',
  }

  return (
    <button
      onClick={onClick}
      className={`p-4 rounded-lg border-2 text-left transition-all ${
        active ? activeStyles[color] : 'border-gray-200 bg-white hover:border-gray-300'
      }`}
    >
      <div className={`text-2xl font-bold ${active ? textStyles[color] : 'text-gray-900'}`}>
        {value}
      </div>
      <div className={`text-xs font-medium uppercase tracking-wide ${active ? textStyles[color] : 'text-gray-500'}`}>
        {label}
      </div>
      {subtext && (
        <div className="text-xs text-gray-400 mt-1">{subtext}</div>
      )}
    </button>
  )
}


function DocumentListItem({ doc, selected, onClick }) {
  const failCount = doc.summary?.failed || 0
  const hasFailures = failCount > 0
  const fv = doc.field_values || {}

  return (
    <div
      onClick={onClick}
      className={`px-4 py-3 border-b border-gray-100 cursor-pointer transition-colors ${
        selected ? 'bg-blue-50 border-l-4 border-l-blue-500' : 'hover:bg-gray-50'
      }`}
    >
      {/* Header row: Employee name + fail badge */}
      <div className="flex items-start justify-between mb-1">
        {fv.employee_name ? (
          <span className="text-sm font-medium text-gray-900">
            {fv.employee_name}
          </span>
        ) : (
          <a
            href={getCredibleLink(doc.document_id)}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm font-medium text-blue-600 hover:text-blue-800 hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            {doc.document_id}
          </a>
        )}
        {hasFailures && (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800">
            {failCount} fail{failCount !== 1 ? 's' : ''}
          </span>
        )}
      </div>

      {/* Field values grid */}
      <div className="text-xs text-gray-500 space-y-1">
        {/* Program */}
        {fv.program && (
          <div className="flex items-center gap-1">
            <span className="text-gray-400">Program:</span>
            <span className="text-gray-600">{fv.program}</span>
          </div>
        )}

        {/* Service type */}
        {fv.service_type && (
          <div className="flex items-center gap-1">
            <span className="text-gray-400">Type:</span>
            <span className="text-gray-600">{fv.service_type}</span>
          </div>
        )}

        {/* Date and CPT on same row */}
        <div className="flex items-center gap-3">
          {fv.date && (
            <div className="flex items-center gap-1">
              <span className="text-gray-400">Date:</span>
              <span className="text-gray-600">{fv.date}</span>
            </div>
          )}
          {fv.cpt_code && (
            <div className="flex items-center gap-1">
              <span className="text-gray-400">CPT:</span>
              <span className="text-gray-600">{fv.cpt_code}</span>
            </div>
          )}
        </div>

        {/* Diagnosis and Rate on same row */}
        <div className="flex items-center gap-3">
          {fv.diagnosis_code && (
            <div className="flex items-center gap-1">
              <span className="text-gray-400">Dx:</span>
              <span className="text-gray-600">{fv.diagnosis_code}</span>
            </div>
          )}
          {fv.rate && (
            <div className="flex items-center gap-1">
              <span className="text-gray-400">Rate:</span>
              <span className="text-gray-600">${fv.rate}</span>
            </div>
          )}
        </div>

        {/* Service ID */}
        {fv.service_id && fv.service_id !== doc.document_id && (
          <div className="flex items-center gap-1">
            <span className="text-gray-400">Service ID:</span>
            <a
              href={getCredibleLink(fv.service_id)}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:text-blue-800 hover:underline font-mono text-[10px]"
              onClick={(e) => e.stopPropagation()}
            >
              {fv.service_id}
            </a>
          </div>
        )}
      </div>

      {/* Rule status indicators */}
      <div className="flex gap-1 mt-2">
        {doc.rules?.slice(0, 8).map((rule, idx) => (
          <div
            key={idx}
            className={`w-2 h-2 rounded-full ${
              rule.status === 'PASS' ? 'bg-green-400' :
              rule.status === 'FAIL' ? 'bg-red-400' :
              'bg-gray-300'
            }`}
            title={`${rule.rule_name}: ${rule.status}`}
          />
        ))}
        {doc.rules?.length > 8 && (
          <span className="text-xs text-gray-400">+{doc.rules.length - 8}</span>
        )}
      </div>
    </div>
  )
}


function DocumentDetailPanel({ doc, selectedRule, onSelectRule }) {
  const failedRules = doc.rules?.filter(r => r.status === 'FAIL') || []
  const passedRules = doc.rules?.filter(r => r.status === 'PASS') || []
  const skippedRules = doc.rules?.filter(r => r.status === 'SKIP') || []

  return (
    <div className="flex flex-col h-full">
      {/* Document Header */}
      <div className="px-4 py-3 border-b border-gray-200 bg-gray-50">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-medium text-gray-900">
              {doc.field_values?.employee_name || 'Document'}
            </h3>
            <p className="text-sm text-gray-500">
              ID:{' '}
              <a
                href={getCredibleLink(doc.document_id)}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:text-blue-800 hover:underline"
              >
                {doc.document_id}
              </a>
            </p>
          </div>
          <div className="text-right text-sm">
            <div className="text-gray-500">{doc.field_values?.program}</div>
            <div className="text-gray-400">{doc.field_values?.date}</div>
          </div>
        </div>
      </div>

      {/* Rules Tabs */}
      <div className="px-4 py-2 border-b border-gray-200 flex gap-4">
        <RuleTab
          label="Failed"
          count={failedRules.length}
          color="red"
          rules={failedRules}
          selectedRule={selectedRule}
          onSelectRule={onSelectRule}
        />
        <RuleTab
          label="Skipped"
          count={skippedRules.length}
          color="gray"
          rules={skippedRules}
          selectedRule={selectedRule}
          onSelectRule={onSelectRule}
        />
        <RuleTab
          label="Passed"
          count={passedRules.length}
          color="green"
          rules={passedRules}
          selectedRule={selectedRule}
          onSelectRule={onSelectRule}
        />
      </div>

      {/* Rule Selector */}
      <div className="px-4 py-2 border-b border-gray-200 bg-gray-50 overflow-x-auto">
        <div className="flex gap-2">
          {doc.rules?.map((rule, idx) => (
            <button
              key={idx}
              onClick={() => onSelectRule(rule)}
              className={`px-3 py-1.5 rounded-full text-xs font-medium whitespace-nowrap transition-colors ${
                selectedRule === rule
                  ? rule.status === 'FAIL' ? 'bg-red-600 text-white' :
                    rule.status === 'PASS' ? 'bg-green-600 text-white' :
                    'bg-gray-500 text-white'
                  : rule.status === 'FAIL' ? 'bg-red-100 text-red-700 hover:bg-red-200' :
                    rule.status === 'PASS' ? 'bg-green-100 text-green-700 hover:bg-green-200' :
                    'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              {rule.rule_name || rule.rule_id}
            </button>
          ))}
        </div>
      </div>

      {/* Selected Rule Detail */}
      <div className="flex-1 overflow-y-auto p-4">
        {selectedRule ? (
          <RuleDetailView rule={selectedRule} fieldValues={doc.field_values} />
        ) : (
          <p className="text-gray-500">Select a rule to view details</p>
        )}
      </div>
    </div>
  )
}


function RuleTab({ label, count, color }) {
  const colorStyles = {
    red: 'text-red-600',
    gray: 'text-gray-600',
    green: 'text-green-600',
  }

  return (
    <div className="flex items-center gap-1.5">
      <span className={`font-medium ${colorStyles[color]}`}>{count}</span>
      <span className="text-sm text-gray-500">{label}</span>
    </div>
  )
}


function RuleDetailView({ rule, fieldValues }) {
  // Extract reasoning from message (format: "STATUS - reasoning")
  const extractReasoning = () => {
    const message = rule.message || ''
    const status = rule.status || ''
    if (message.startsWith(`${status} - `)) {
      return message.substring(status.length + 3)
    }
    if (message.startsWith(`${status}: `)) {
      return message.substring(status.length + 2)
    }
    return message || 'No reasoning provided.'
  }

  const statusColors = {
    FAIL: { bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-200' },
    PASS: { bg: 'bg-green-100', text: 'text-green-800', border: 'border-green-200' },
    SKIP: { bg: 'bg-gray-100', text: 'text-gray-700', border: 'border-gray-200' },
    ERROR: { bg: 'bg-gray-100', text: 'text-gray-800', border: 'border-gray-200' },
  }

  const colors = statusColors[rule.status] || statusColors.ERROR

  return (
    <div className="space-y-4">
      {/* Tags */}
      <div className="flex flex-wrap gap-2">
        <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium ${colors.bg} ${colors.text}`}>
          {rule.status}
        </span>
        {rule.category && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-blue-100 text-blue-800">
            {rule.category}
          </span>
        )}
        {rule.status === 'FAIL' && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-orange-100 text-orange-800">
            BILLING BLOCKER
          </span>
        )}
      </div>

      {/* Rule Name */}
      <div>
        <h4 className="text-lg font-semibold text-gray-900">{rule.rule_name || rule.rule_id}</h4>
        {rule.rule_id && rule.rule_name && (
          <p className="text-sm text-gray-500">Rule ID: {rule.rule_id}</p>
        )}
      </div>

      {/* Reasoning */}
      <div className={`p-4 rounded-lg border ${colors.border} ${colors.bg}`}>
        <h5 className="text-sm font-medium text-gray-700 mb-2">Reasoning</h5>
        <p className="text-sm text-gray-800">{extractReasoning()}</p>
      </div>

      {/* Recommended Next Steps */}
      {rule.status === 'FAIL' && (
        <div className="p-4 rounded-lg border border-blue-200 bg-blue-50">
          <h5 className="text-sm font-medium text-blue-800 mb-2">Recommended Next Step</h5>
          <p className="text-sm text-blue-700">
            Review the documentation for this service to verify compliance with billing requirements.
            Update the chart if corrections are needed before resubmitting for validation.
          </p>
        </div>
      )}

      {/* Evidence / Field Values */}
      {fieldValues && Object.keys(fieldValues).length > 0 && (
        <div className="p-4 rounded-lg border border-gray-200 bg-gray-50">
          <h5 className="text-sm font-medium text-gray-700 mb-3">Evidence (Field Values)</h5>
          <div className="grid grid-cols-2 gap-3">
            {Object.entries(fieldValues).map(([key, value]) => (
              value && (
                <div key={key}>
                  <dt className="text-xs text-gray-500 uppercase tracking-wide">
                    {FIELD_LABELS[key] || key}
                  </dt>
                  <dd className="text-sm text-gray-900 font-medium">{value}</dd>
                </div>
              )
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
