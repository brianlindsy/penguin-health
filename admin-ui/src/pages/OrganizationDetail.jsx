import { useState, useEffect, useMemo, useCallback } from 'react'
import { useParams, Link, useSearchParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { StatusBadge } from '../components/StatusBadge.jsx'
import { ValidationStatusBadge } from '../components/ValidationStatusBadge.jsx'
import { JsonEditor } from '../components/JsonEditor.jsx'
import { RunCategories } from '../components/RunCategories.jsx'
import { RunDates } from '../components/RunDates.jsx'
import { RunTimestamp } from '../components/RunTimestamp.jsx'
import { usePermissions } from '../auth/usePermissions.js'

const TABS = ['Rules', 'Field Mappings', 'UI Display Fields']
const TAB_PARAM_MAP = {
  'rules': 'Rules',
  'field-mappings': 'Field Mappings',
  'ui-display-fields': 'UI Display Fields',
}

export function OrganizationDetail() {
  const { orgId } = useParams()
  const [searchParams] = useSearchParams()
  const tabParam = searchParams.get('tab')
  const initialTab = TAB_PARAM_MAP[tabParam] || 'Rules'
  const headerPerms = usePermissions()

  const [org, setOrg] = useState(null)
  const [rules, setRules] = useState([])
  const [rulesConfig, setRulesConfig] = useState(null)
  const [activeTab, setActiveTab] = useState(initialTab)
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
            to={`/organizations/${orgId}/analytics?tab=staff-performance`}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
            </svg>
            Staff Performance
          </Link>
          <Link
            to={`/organizations/${orgId}/eligibility`}
            className="flex items-center gap-2 px-4 py-2 bg-white border border-blue-600 text-blue-600 rounded-lg text-sm font-medium hover:bg-blue-50 transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Insurance Eligibility
          </Link>
          <Link
            to={`/organizations/${orgId}/eligibility/worklist`}
            className="relative flex items-center gap-2 px-4 py-2 bg-white border border-blue-600 text-blue-600 rounded-lg text-sm font-medium hover:bg-blue-50 transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17v-2a4 4 0 014-4h4M5 21h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v14a2 2 0 002 2z" />
            </svg>
            Eligibility Worklist
            {headerPerms.eligibilityUnreadCount > 0 && (
              <span className="absolute -top-2 -right-2 inline-flex items-center justify-center min-w-[1.25rem] h-5 px-1 rounded-full bg-red-500 text-white text-xs font-semibold">
                {headerPerms.eligibilityUnreadCount}
              </span>
            )}
          </Link>
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

      {activeTab === 'UI Display Fields' && (
        <UIDisplayFieldsTab orgId={orgId} />
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


// Canonical UI field names the DocumentQueuePage knows how to render.
// Kept in sync with FIELD_LABELS + MULTI_FILTER_FIELDS in that page and with
// KNOWN_UI_FIELDS in scripts/multi-org/seed_ui_display_fields.py. Anything
// not in this list still saves (the editor is free-form), but the UI won't
// have a nice label for it.
const KNOWN_UI_FIELDS = [
  'service_id', 'date', 'program', 'service_type', 'diagnosis_code',
  'bed_day_diagnosis_code', 'cpt_code', 'rate', 'employee_name',
  'document_id', 'payer_description',
]

function UIDisplayFieldsTab({ orgId }) {
  const [mappings, setMappings] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveMsg, setSaveMsg] = useState('')

  useEffect(() => {
    setLoading(true)
    api.getUiDisplayFields(orgId)
      .then(data => setMappings(data?.mappings || {}))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  const handleSave = async () => {
    setSaving(true)
    setSaveMsg('')
    try {
      await api.updateUiDisplayFields(orgId, mappings)
      setSaveMsg('Saved')
      setTimeout(() => setSaveMsg(''), 3000)
    } catch (err) {
      setSaveMsg(`Error: ${err.message}`)
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <p className="text-gray-500">Loading UI display fields...</p>
  if (error) return <p className="text-red-600">Error: {error}</p>

  const unknownKeys = Object.keys(mappings || {}).filter(
    k => !KNOWN_UI_FIELDS.includes(k)
  )

  return (
    <div className="max-w-2xl space-y-6">
      <div>
        <h2 className="text-lg font-medium text-gray-900 mb-2">UI Display Fields</h2>
        <p className="text-sm text-gray-500 mb-2">
          Maps canonical UI field names to the source field this org's
          validation results carry, so charts render with consistent labels
          across orgs even when the underlying data uses different names.
        </p>
        <p className="text-sm text-gray-500 mb-4">
          For example, <code className="bg-gray-100 px-1 rounded">"employee_name": "provider_display"</code>
          {' '}tells the UI to show <code className="bg-gray-100 px-1 rounded">provider_display</code>{' '}
          from each document's <code className="bg-gray-100 px-1 rounded">field_values</code>{' '}
          wherever a "Employee" column is displayed. An empty object turns
          projection off — the UI falls back to reading raw{' '}
          <code className="bg-gray-100 px-1 rounded">field_values</code> keys directly.
        </p>
        <p className="text-xs text-gray-400 mb-3">
          Known canonical fields: {KNOWN_UI_FIELDS.join(', ')}
        </p>

        <JsonEditor
          value={mappings || {}}
          onChange={setMappings}
          label="mappings (canonical → source)"
        />

        {unknownKeys.length > 0 && (
          <p className="text-xs text-yellow-700 mt-2">
            Warning: canonical name(s) not recognized by the UI:{' '}
            <code>{unknownKeys.join(', ')}</code>. They'll save fine but the
            UI has no built-in label for them.
          </p>
        )}
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={handleSave}
          disabled={saving}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {saving ? 'Saving...' : 'Save Display Fields'}
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


