import { useState, useEffect, useMemo } from 'react'
import { useParams, Link, useSearchParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { StatusBadge } from '../components/StatusBadge.jsx'
import { ValidationStatusBadge } from '../components/ValidationStatusBadge.jsx'
import { JsonEditor } from '../components/JsonEditor.jsx'
import { RunCategories } from '../components/RunCategories.jsx'
import { usePermissions } from '../auth/usePermissions.js'

const TABS = ['Rules', 'Field Mappings', 'Validation Results']
const TAB_PARAM_MAP = {
  'validation': 'Validation Results',
  'rules': 'Rules',
  'field-mappings': 'Field Mappings',
}

export function OrganizationDetail() {
  const { orgId } = useParams()
  const [searchParams] = useSearchParams()
  const tabParam = searchParams.get('tab')
  const initialTab = TAB_PARAM_MAP[tabParam] || 'Rules'

  const [org, setOrg] = useState(null)
  const [rules, setRules] = useState([])
  const [rulesConfig, setRulesConfig] = useState(null)
  const [activeTab, setActiveTab] = useState(initialTab)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveMsg, setSaveMsg] = useState('')
  const [latestRunId, setLatestRunId] = useState(null)

  useEffect(() => {
    Promise.all([
      api.getOrganization(orgId),
      api.listRules(orgId),
      api.getRulesConfig(orgId),
    ])
      .then(([orgData, rulesData, configData]) => {
        setOrg(orgData)
        setRules(rulesData.rules || [])
        setRulesConfig(configData)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  // Grab the most recent validation run so the header can deep-link to it.
  // Kept separate from the main load so a failure here doesn't blow up the page.
  useEffect(() => {
    let cancelled = false
    api.listValidationRuns(orgId)
      .then(data => {
        if (cancelled) return
        const runs = (Array.isArray(data) ? data : data?.runs) || []
        if (runs.length === 0) return
        const sorted = [...runs].sort((a, b) => {
          const at = a.timestamp ? new Date(a.timestamp).getTime() : 0
          const bt = b.timestamp ? new Date(b.timestamp).getTime() : 0
          return bt - at
        })
        setLatestRunId(sorted[0].validation_run_id)
      })
      .catch(() => { /* silent — button stays disabled */ })
    return () => { cancelled = true }
  }, [orgId])

  const handleSaveRulesConfig = async (configUpdate) => {
    setSaving(true)
    setSaveMsg('')
    try {
      await api.updateRulesConfig(orgId, configUpdate)
      // Update local state with saved values
      setRulesConfig(prev => ({ ...prev, ...configUpdate }))
      setSaveMsg('Saved')
      setTimeout(() => setSaveMsg(''), 3000)
    } catch (err) {
      setSaveMsg(`Error: ${err.message}`)
    } finally {
      setSaving(false)
    }
  }

  const handleToggleRule = async (rule) => {
    try {
      const updated = await api.updateRule(orgId, rule.rule_id, {
        enabled: !rule.enabled,
      })
      setRules(prev => prev.map(r => r.rule_id === rule.rule_id ? updated : r))
    } catch (err) {
      setError(err.message)
    }
  }

  if (loading) return <p className="text-gray-500">Loading organization...</p>
  if (error) return <p className="text-red-600">Error: {error}</p>
  if (!org) return <p className="text-gray-500">Organization not found</p>

  return (
    <div>
      {/* Header */}
      <div className="mb-6 flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">{org.organization_name}</h1>
          <div className="flex items-center gap-4 mt-2">
            <span className="text-sm text-gray-500 font-mono">{org.organization_id}</span>
            <StatusBadge enabled={org.enabled} />
            <span className="text-sm text-gray-500">{org.s3_bucket_name}</span>
          </div>
        </div>
        <div className="flex flex-col items-end gap-2">
          <Link
            to={`/organizations/${orgId}/staff-performance`}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
            </svg>
            Staff Performance
          </Link>
          {latestRunId ? (
            <Link
              to={`/organizations/${orgId}/validation-runs/${latestRunId}`}
              className="flex items-center gap-2 px-4 py-2 bg-white border border-blue-600 text-blue-600 rounded-lg text-sm font-medium hover:bg-blue-50 transition-colors"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
              Today's Validation
            </Link>
          ) : (
            <span
              className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 text-gray-400 rounded-lg text-sm font-medium cursor-not-allowed"
              title="No validation runs yet"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
              Today's Validation
            </span>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200 mb-6">
        <nav className="flex gap-8">
          {TABS.map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`py-3 text-sm font-medium border-b-2 ${
                activeTab === tab
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      {activeTab === 'Rules' && (
        <RulesTab orgId={orgId} rules={rules} onToggle={handleToggleRule} />
      )}

      {activeTab === 'Field Mappings' && (
        <FieldMappingsTab
          config={rulesConfig}
          onSave={handleSaveRulesConfig}
          saving={saving}
          saveMsg={saveMsg}
        />
      )}

      {activeTab === 'Validation Results' && (
        <ValidationResultsTab orgId={orgId} />
      )}
    </div>
  )
}


function RulesTab({ orgId, rules, onToggle }) {
  const perms = usePermissions()
  const sorted = [...rules].sort((a, b) => {
    const aNum = parseInt(a.rule_id) || 0
    const bNum = parseInt(b.rule_id) || 0
    return aNum - bNum
  })

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium text-gray-900">
          Validation Rules ({rules.length})
        </h2>
        {perms.isOrgAdmin && (
          <Link
            to={`/organizations/${orgId}/rules/new`}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
          >
            Add Rule
          </Link>
        )}
      </div>

      <div className="bg-white shadow rounded-lg overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-16">ID</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Type</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Status</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {sorted.map(rule => (
              <tr key={rule.rule_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 text-sm font-mono text-gray-600">{rule.rule_id}</td>
                <td className="px-4 py-3">
                  <Link
                    to={`/organizations/${orgId}/rules/${rule.rule_id}`}
                    className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                  >
                    {rule.name}
                  </Link>
                </td>
                <td className="px-4 py-3 text-sm text-gray-600">{rule.category}</td>
                <td className="px-4 py-3 text-sm text-gray-600">{rule.type}</td>
                <td className="px-4 py-3">
                  <StatusBadge enabled={rule.enabled} />
                </td>
                <td className="px-4 py-3">
                  <button
                    onClick={() => onToggle(rule)}
                    className="text-xs text-gray-500 hover:text-gray-700"
                  >
                    {rule.enabled ? 'Disable' : 'Enable'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}


function FieldMappingsTab({ config, onSave, saving, saveMsg }) {
  const [fieldMappings, setFieldMappings] = useState(config?.field_mappings || {})
  const [csvColumnMappings, setCsvColumnMappings] = useState(config?.csv_column_mappings || {})

  const handleSave = () => {
    onSave({
      field_mappings: fieldMappings,
      csv_column_mappings: csvColumnMappings,
    })
  }

  return (
    <div className="max-w-2xl space-y-8">
      {/* Text Field Mappings (for PDFs) */}
      <div>
        <h2 className="text-lg font-medium text-gray-900 mb-2">Text Field Mappings (PDFs)</h2>
        <p className="text-sm text-gray-500 mb-4">
          Maps field names to text patterns used to extract values from PDF/Textract documents.
          For example, <code className="bg-gray-100 px-1 rounded">document_id</code> maps to the
          text label in the document like "Consumer Service ID:".
        </p>

        <JsonEditor
          value={fieldMappings}
          onChange={setFieldMappings}
          label="field_mappings"
        />
      </div>

      {/* CSV Column Mappings */}
      <div>
        <h2 className="text-lg font-medium text-gray-900 mb-2">CSV Column Mappings</h2>
        <p className="text-sm text-gray-500 mb-4">
          Maps internal field names to CSV column names for SFTP-uploaded data.
          Use <code className="bg-gray-100 px-1 rounded">null</code> for fields not available in this org's CSV format.
        </p>
        <p className="text-xs text-gray-400 mb-3">
          Standard fields: service_id, date, program, service_type, diagnosis_code, cpt_code, rate, employee_name
        </p>

        <JsonEditor
          value={csvColumnMappings}
          onChange={setCsvColumnMappings}
          label="csv_column_mappings"
        />
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={handleSave}
          disabled={saving}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {saving ? 'Saving...' : 'Save All Mappings'}
        </button>
        {saveMsg && (
          <span className={`text-sm ${saveMsg.startsWith('Error') ? 'text-red-600' : 'text-green-600'}`}>
            {saveMsg}
          </span>
        )}
      </div>
    </div>
  )
}


function ValidationResultsTab({ orgId }) {
  const perms = usePermissions()
  const runnable = useMemo(() => Array.from(perms.runnableCategories()).sort(),
    [perms])
  const canRunAny = runnable.length > 0

  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [triggering, setTriggering] = useState(false)
  const [triggerMsg, setTriggerMsg] = useState('')
  const [periodFilter, setPeriodFilter] = useState('all')
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')
  // Default to running every category the user is allowed to run.
  const [selectedCategories, setSelectedCategories] = useState(runnable)
  const [pickerOpen, setPickerOpen] = useState(false)

  // Keep `selectedCategories` aligned with `runnable` once permissions arrive.
  useEffect(() => {
    setSelectedCategories(runnable)
  }, [runnable.join('|')])

  const loadRuns = () => {
    setLoading(true)
    api.listValidationRuns(orgId)
      .then(data => setRuns(data.runs || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    loadRuns()
  }, [orgId])

  const toggleCategory = (cat) => {
    setSelectedCategories(prev =>
      prev.includes(cat) ? prev.filter(c => c !== cat) : [...prev, cat]
    )
  }

  const handleTriggerValidation = async () => {
    if (selectedCategories.length === 0) {
      setTriggerMsg('Error: select at least one category')
      return
    }
    setTriggering(true)
    setTriggerMsg('')
    setPickerOpen(false)
    try {
      await api.triggerValidationRun(orgId, selectedCategories)
      setTriggerMsg('Validation run started. Results will appear shortly.')
      setTimeout(() => {
        loadRuns()
        setTriggerMsg('')
      }, 5000)
    } catch (err) {
      setTriggerMsg(`Error: ${err.message}`)
    } finally {
      setTriggering(false)
    }
  }

  // A run's "date that it took place" = its `timestamp` from listValidationRuns.
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
      // End cutoff is exclusive — include the full end day.
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

  if (loading) return <p className="text-gray-500">Loading validation runs...</p>
  if (error) return <p className="text-red-600">Error: {error}</p>

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium text-gray-900">
          Validation Runs ({filteredRuns.length}{filteredRuns.length !== runs.length ? ` of ${runs.length}` : ''})
        </h2>
        <div className="flex items-center gap-3">
          {triggerMsg && (
            <span className={`text-sm ${triggerMsg.startsWith('Error') ? 'text-red-600' : 'text-green-600'}`}>
              {triggerMsg}
            </span>
          )}
          {canRunAny && (
            <div className="relative flex items-center gap-2">
              <button
                onClick={() => setPickerOpen(o => !o)}
                disabled={triggering}
                className="px-3 py-2 text-xs text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50"
                title="Choose rule categories to validate"
              >
                {selectedCategories.length === runnable.length
                  ? 'All categories'
                  : `${selectedCategories.length} of ${runnable.length} categories`}
                <span className="ml-1 text-gray-400">▾</span>
              </button>
              {pickerOpen && (
                <div className="absolute right-32 top-full mt-1 z-10 bg-white border border-gray-200 rounded-md shadow-lg p-3 w-56">
                  <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                    Categories
                  </div>
                  {runnable.map(cat => (
                    <label key={cat} className="flex items-center gap-2 py-1 text-sm cursor-pointer">
                      <input
                        type="checkbox"
                        checked={selectedCategories.includes(cat)}
                        onChange={() => toggleCategory(cat)}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                      <span className="text-gray-700">{cat}</span>
                    </label>
                  ))}
                </div>
              )}
              <button
                onClick={handleTriggerValidation}
                disabled={triggering || selectedCategories.length === 0}
                className="px-4 py-2 bg-green-600 text-white text-sm rounded-md hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
              >
                {triggering ? (
                  <>
                    <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                    </svg>
                    Running...
                  </>
                ) : (
                  'Run Validation'
                )}
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Date filter */}
      <div className="bg-white rounded-lg border border-gray-200 p-3 mb-4 flex flex-wrap items-end gap-3">
        <div className="flex flex-col">
          <label className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">Date</label>
          <select
            value={periodFilter}
            onChange={(e) => setPeriodFilter(e.target.value)}
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
          </>
        )}
        {periodFilter !== 'all' && (
          <button
            onClick={() => {
              setPeriodFilter('all')
              setCustomStartDate('')
              setCustomEndDate('')
            }}
            className="text-sm text-blue-600 hover:text-blue-800 px-2 py-2"
          >
            Clear
          </button>
        )}
      </div>

      {runs.length === 0 ? (
        <p className="text-gray-500">No validation runs found for this organization.</p>
      ) : filteredRuns.length === 0 ? (
        <p className="text-gray-500">No validation runs in the selected date range.</p>
      ) : (
        <div className="bg-white shadow rounded-lg overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Run ID</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Categories</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Docs</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Pass</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Fail</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Skip</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredRuns.map(run => (
                <tr key={run.validation_run_id} className="hover:bg-gray-50">
                  <td className="px-4 py-3">
                    <Link
                      to={`/organizations/${orgId}/validation-runs/${run.validation_run_id}`}
                      className="text-blue-600 hover:text-blue-800 text-sm font-mono"
                    >
                      {run.validation_run_id}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {run.timestamp ? new Date(run.timestamp).toLocaleString() : '-'}
                  </td>
                  <td className="px-4 py-3">
                    <RunCategories categories={run.categories} />
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900 font-medium">{run.total_documents}</td>
                  <td className="px-4 py-3 text-sm text-green-600 font-medium">{run.passed}</td>
                  <td className="px-4 py-3 text-sm text-red-600 font-medium">{run.failed}</td>
                  <td className="px-4 py-3 text-sm text-yellow-600 font-medium">{run.skipped}</td>
                  <td className="px-4 py-3">
                    <ValidationStatusBadge status={run.failed > 0 ? 'FAIL' : 'PASS'} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
