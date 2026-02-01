import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { api } from '../api/client.js'
import { JsonEditor } from '../components/JsonEditor.jsx'

export function RuleEditor() {
  const { orgId, ruleId } = useParams()
  const navigate = useNavigate()
  const isNew = ruleId === 'new'

  const [rule, setRule] = useState({
    id: '',
    name: '',
    category: '',
    description: '',
    enabled: true,
    type: 'llm',
    version: '1.0.0',
    llm_config: {
      model_id: '',
      system_prompt: '',
      question: '',
      use_rag: false,
      knowledge_base_id: '',
    },
    messages: {
      pass: 'PASS',
      fail: 'FAIL: {llm_reasoning}',
      skip: 'SKIP: {llm_reasoning}',
    },
  })

  const [loading, setLoading] = useState(!isNew)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [saveMsg, setSaveMsg] = useState('')

  useEffect(() => {
    if (!isNew) {
      api.getRule(orgId, ruleId)
        .then(data => {
          setRule({
            id: data.rule_id,
            name: data.name,
            category: data.category,
            description: data.description || '',
            enabled: data.enabled,
            type: data.type,
            version: data.version,
            llm_config: data.llm_config || {},
            messages: data.messages || {},
          })
        })
        .catch(err => setError(err.message))
        .finally(() => setLoading(false))
    }
  }, [orgId, ruleId, isNew])

  const handleSave = async () => {
    setError('')
    setSaveMsg('')
    setSaving(true)

    try {
      if (isNew) {
        if (!rule.id) {
          setError('Rule ID is required')
          setSaving(false)
          return
        }
        await api.createRule(orgId, rule)
        navigate(`/organizations/${orgId}/rules/${rule.id}`, { replace: true })
      } else {
        await api.updateRule(orgId, ruleId, rule)
        setSaveMsg('Saved')
        setTimeout(() => setSaveMsg(''), 3000)
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  const updateField = (field, value) => {
    setRule(prev => ({ ...prev, [field]: value }))
  }

  if (loading) return <p className="text-gray-500">Loading rule...</p>

  return (
    <div className="max-w-3xl">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-semibold text-gray-900">
          {isNew ? 'New Rule' : `Edit Rule: ${rule.name}`}
        </h1>
        <button
          onClick={() => navigate(`/organizations/${orgId}`)}
          className="text-sm text-gray-500 hover:text-gray-700"
        >
          Back to Organization
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 text-sm rounded">{error}</div>
      )}

      <div className="bg-white shadow rounded-lg p-6 space-y-6">
        {/* Basic fields */}
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Rule ID</label>
            <input
              type="text"
              value={rule.id}
              onChange={e => updateField('id', e.target.value)}
              disabled={!isNew}
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm disabled:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Version</label>
            <input
              type="text"
              value={rule.version}
              onChange={e => updateField('version', e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
          <input
            type="text"
            value={rule.name}
            onChange={e => updateField('name', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Category</label>
            <input
              type="text"
              value={rule.category}
              onChange={e => updateField('category', e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Type</label>
            <input
              type="text"
              value={rule.type}
              onChange={e => updateField('type', e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
          <textarea
            value={rule.description}
            onChange={e => updateField('description', e.target.value)}
            rows={3}
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="enabled"
            checked={rule.enabled}
            onChange={e => updateField('enabled', e.target.checked)}
            className="rounded border-gray-300"
          />
          <label htmlFor="enabled" className="text-sm text-gray-700">Enabled</label>
        </div>

        {/* LLM Config */}
        <JsonEditor
          value={rule.llm_config}
          onChange={v => updateField('llm_config', v)}
          label="LLM Configuration"
        />

        {/* Messages */}
        <JsonEditor
          value={rule.messages}
          onChange={v => updateField('messages', v)}
          label="Messages"
        />

        {/* Save */}
        <div className="flex items-center gap-3 pt-4 border-t">
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {saving ? 'Saving...' : isNew ? 'Create Rule' : 'Save Changes'}
          </button>
          {saveMsg && (
            <span className="text-sm text-green-600">{saveMsg}</span>
          )}
        </div>
      </div>
    </div>
  )
}
