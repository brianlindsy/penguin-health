import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { StatusBadge } from '../components/StatusBadge.jsx'
import { ValidationStatusBadge } from '../components/ValidationStatusBadge.jsx'
import { JsonEditor } from '../components/JsonEditor.jsx'

const TABS = ['Rules', 'Field Mappings', 'Validation Results']

export function OrganizationDetail() {
  const { orgId } = useParams()
  const [org, setOrg] = useState(null)
  const [rules, setRules] = useState([])
  const [rulesConfig, setRulesConfig] = useState(null)
  const [activeTab, setActiveTab] = useState('Rules')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveMsg, setSaveMsg] = useState('')

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
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-gray-900">{org.organization_name}</h1>
        <div className="flex items-center gap-4 mt-2">
          <span className="text-sm text-gray-500 font-mono">{org.organization_id}</span>
          <StatusBadge enabled={org.enabled} />
          <span className="text-sm text-gray-500">{org.s3_bucket_name}</span>
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
        <Link
          to={`/organizations/${orgId}/rules/new`}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          Add Rule
        </Link>
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
  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [triggering, setTriggering] = useState(false)
  const [triggerMsg, setTriggerMsg] = useState('')

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

  const handleTriggerValidation = async () => {
    setTriggering(true)
    setTriggerMsg('')
    try {
      await api.triggerValidationRun(orgId)
      setTriggerMsg('Validation run started. Results will appear shortly.')
      // Refresh the list after a short delay to show the new run
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

  if (loading) return <p className="text-gray-500">Loading validation runs...</p>
  if (error) return <p className="text-red-600">Error: {error}</p>

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium text-gray-900">
          Validation Runs ({runs.length})
        </h2>
        <div className="flex items-center gap-3">
          {triggerMsg && (
            <span className={`text-sm ${triggerMsg.startsWith('Error') ? 'text-red-600' : 'text-green-600'}`}>
              {triggerMsg}
            </span>
          )}
          <button
            onClick={handleTriggerValidation}
            disabled={triggering}
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
      </div>

      {runs.length === 0 ? (
        <p className="text-gray-500">No validation runs found for this organization.</p>
      ) : (
        <div className="bg-white shadow rounded-lg overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Run ID</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Docs</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Pass</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Fail</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Skip</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {runs.map(run => (
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
