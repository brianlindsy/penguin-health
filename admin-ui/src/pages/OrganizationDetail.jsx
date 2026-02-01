import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { StatusBadge } from '../components/StatusBadge.jsx'
import { JsonEditor } from '../components/JsonEditor.jsx'

const TABS = ['Rules', 'Field Mappings']

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

  const handleSaveFieldMappings = async (fieldMappings) => {
    setSaving(true)
    setSaveMsg('')
    try {
      await api.updateRulesConfig(orgId, { field_mappings: fieldMappings })
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
          onSave={handleSaveFieldMappings}
          saving={saving}
          saveMsg={saveMsg}
        />
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
  const [mappings, setMappings] = useState(config?.field_mappings || {})

  return (
    <div className="max-w-2xl">
      <h2 className="text-lg font-medium text-gray-900 mb-2">Field Mappings</h2>
      <p className="text-sm text-gray-500 mb-4">
        Maps field names to text patterns used to extract values from documents.
        For example, <code className="bg-gray-100 px-1 rounded">document_id</code> maps to the
        text label in the document like "Consumer Service ID:".
      </p>

      <JsonEditor
        value={mappings}
        onChange={setMappings}
        label="field_mappings"
      />

      <div className="flex items-center gap-3 mt-4">
        <button
          onClick={() => onSave(mappings)}
          disabled={saving}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {saving ? 'Saving...' : 'Save'}
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
