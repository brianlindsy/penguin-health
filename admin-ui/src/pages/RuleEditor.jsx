import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { api } from '../api/client.js'
import { JsonEditor } from '../components/JsonEditor.jsx'

export function RuleEditor() {
  const { orgId, ruleId } = useParams()
  const navigate = useNavigate()

  const [rule, setRule] = useState({
    id: '',
    name: '',
    category: '',
    description: '',
    enabled: true,
    type: 'llm',
    version: '1.0.0',
    rule_text: '',
    fields_to_extract: [],
    notes: [],
  })

  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [enhancing, setEnhancing] = useState(false)
  const [newNote, setNewNote] = useState('')
  const [error, setError] = useState('')
  const [saveMsg, setSaveMsg] = useState('')

  useEffect(() => {
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
          rule_text: data.rule_text || '',
          fields_to_extract: data.fields_to_extract || [],
          notes: data.notes || [],
        })
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId, ruleId])

  const handleSave = async () => {
    setError('')
    setSaveMsg('')
    setSaving(true)

    try {
      await api.updateRule(orgId, ruleId, rule)
      setSaveMsg('Saved')
      setTimeout(() => setSaveMsg(''), 3000)
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  const updateField = (field, value) => {
    setRule(prev => ({ ...prev, [field]: value }))
  }

  const handleGenerateFields = async () => {
    if (!rule.rule_text.trim()) {
      setError('Enter rule text before generating fields')
      return
    }

    setGenerating(true)
    setError('')

    try {
      const result = await api.enhanceRuleFields(orgId, rule.rule_text)
      updateField('fields_to_extract', result.fields_to_extract)
    } catch (err) {
      setError(`Failed to generate fields: ${err.message}`)
    } finally {
      setGenerating(false)
    }
  }

  const handleAddNote = async () => {
    if (!newNote.trim()) return

    setEnhancing(true)
    setError('')

    try {
      const result = await api.enhanceNote(orgId, newNote, rule.rule_text)
      setRule(prev => ({
        ...prev,
        notes: [...prev.notes, result.enhanced_note],
      }))
      setNewNote('')
    } catch (err) {
      setError(`Failed to enhance note: ${err.message}`)
    } finally {
      setEnhancing(false)
    }
  }

  const handleRemoveNote = (index) => {
    setRule(prev => ({
      ...prev,
      notes: prev.notes.filter((_, i) => i !== index),
    }))
  }

  if (loading) return <p className="text-gray-500">Loading rule...</p>

  return (
    <div className="max-w-3xl">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-semibold text-gray-900">
          Edit Rule: {rule.name}
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
              disabled
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
            rows={2}
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

        {/* Rule Text */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Rule Text
            <span className="text-red-500 ml-1">*</span>
          </label>
          <p className="text-xs text-gray-500 mb-2">
            The validation logic and criteria for this rule. Describe what the LLM should check.
          </p>
          <textarea
            value={rule.rule_text}
            onChange={e => updateField('rule_text', e.target.value)}
            rows={8}
            placeholder="Describe the validation rule, including failure conditions and pass criteria..."
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <div className="mt-2 flex justify-end">
            <button
              onClick={handleGenerateFields}
              disabled={generating || !rule.rule_text.trim()}
              className="px-4 py-2 bg-purple-600 text-white text-sm rounded-md hover:bg-purple-700 disabled:opacity-50"
            >
              {generating ? 'Generating...' : 'Generate Fields from Rule Text'}
            </button>
          </div>
        </div>

        {/* Fields to Extract */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Fields to Extract</label>
          <p className="text-xs text-gray-500 mb-2">
            List of fields the LLM should extract from the chart text before validating.
            Each field needs a name, type (string/number), and description.
          </p>
          <JsonEditor
            value={rule.fields_to_extract}
            onChange={v => updateField('fields_to_extract', v)}
            label=""
            placeholder={`[
  { "name": "recipient", "type": "string", "description": "The Recipient field from the header" },
  { "name": "service_location", "type": "string", "description": "Service Location field" }
]`}
          />
        </div>

        {/* Notes */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Notes</label>
          <p className="text-xs text-gray-500 mb-2">
            Contextual notes to help the LLM understand the rule. Notes are enhanced by AI when added.
          </p>

          <div className="flex gap-2 mb-4">
            <input
              type="text"
              value={newNote}
              onChange={e => setNewNote(e.target.value)}
              placeholder="Add a contextual note..."
              className="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              onKeyDown={e => e.key === 'Enter' && handleAddNote()}
            />
            <button
              onClick={handleAddNote}
              disabled={enhancing || !newNote.trim()}
              className="px-4 py-2 bg-purple-600 text-white text-sm rounded-md hover:bg-purple-700 disabled:opacity-50 whitespace-nowrap"
            >
              {enhancing ? 'Enhancing...' : 'Add & Enhance'}
            </button>
          </div>

          {rule.notes.length > 0 && (
            <ul className="space-y-2 mb-4">
              {rule.notes.map((note, index) => (
                <li key={index} className="flex items-start gap-2 bg-gray-50 p-3 rounded-md">
                  <span className="flex-1 text-sm text-gray-700">{note}</span>
                  <button
                    onClick={() => handleRemoveNote(index)}
                    className="text-xs text-red-500 hover:text-red-700"
                  >
                    Remove
                  </button>
                </li>
              ))}
            </ul>
          )}

          <p className="text-xs text-gray-400">
            Or edit the JSON directly:
          </p>
          <JsonEditor
            value={rule.notes}
            onChange={v => updateField('notes', v)}
            label=""
            rows={4}
            placeholder={`[
  "'Face to Face' Recipient covers In-Person AND Video (Visual) Telehealth.",
  "'Telephone' Recipient is for Audio-Only."
]`}
          />
        </div>

        {/* Save */}
        <div className="flex items-center gap-3 pt-4 border-t">
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {saving ? 'Saving...' : 'Save Changes'}
          </button>
          {saveMsg && (
            <span className="text-sm text-green-600">{saveMsg}</span>
          )}
        </div>
      </div>
    </div>
  )
}
