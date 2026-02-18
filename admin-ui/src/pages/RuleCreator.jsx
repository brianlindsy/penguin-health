import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { api } from '../api/client.js'
import { JsonEditor } from '../components/JsonEditor.jsx'

export function RuleCreator() {
  const { orgId } = useParams()
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

  const [newNote, setNewNote] = useState('')
  const [saving, setSaving] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [enhancing, setEnhancing] = useState(false)
  const [error, setError] = useState('')

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

  const handleCreate = async () => {
    setError('')

    if (!rule.id.trim()) {
      setError('Rule ID is required')
      return
    }
    if (!rule.name.trim()) {
      setError('Rule name is required')
      return
    }
    if (!rule.category.trim()) {
      setError('Category is required')
      return
    }
    if (!rule.rule_text.trim()) {
      setError('Rule text is required')
      return
    }

    setSaving(true)

    try {
      await api.createRule(orgId, rule)
      navigate(`/organizations/${orgId}/rules/${rule.id}`)
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="max-w-3xl">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-semibold text-gray-900">Create New Rule</h1>
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

      <div className="bg-white shadow rounded-lg p-6 space-y-8">
        {/* Step 1: Basic Info */}
        <div>
          <h2 className="text-lg font-medium text-gray-900 mb-4">Step 1: Basic Info</h2>
          <div className="grid grid-cols-3 gap-4 mb-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Rule ID <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={rule.id}
                onChange={e => updateField('id', e.target.value)}
                placeholder="e.g., 1, 2, rule-name"
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Name <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={rule.name}
                onChange={e => updateField('name', e.target.value)}
                placeholder="Rule name"
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Category <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={rule.category}
                onChange={e => updateField('category', e.target.value)}
                placeholder="e.g., Compliance Audit"
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
              placeholder="Brief description of what this rule validates"
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        {/* Step 2: Rule Text */}
        <div className="border-t pt-6">
          <h2 className="text-lg font-medium text-gray-900 mb-4">Step 2: Rule Text</h2>
          <p className="text-sm text-gray-500 mb-2">
            Describe the validation logic and criteria. Include failure conditions and pass criteria.
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

        {/* Step 3: Fields to Extract */}
        <div className="border-t pt-6">
          <h2 className="text-lg font-medium text-gray-900 mb-4">Step 3: Fields to Extract</h2>
          <p className="text-sm text-gray-500 mb-2">
            Fields the LLM should extract from chart documents before validating.
            Use "Generate Fields" above to auto-populate, or edit manually.
          </p>
          <JsonEditor
            value={rule.fields_to_extract}
            onChange={v => updateField('fields_to_extract', v)}
            placeholder={`[
  { "name": "recipient", "type": "string", "description": "The Recipient field from the header" }
]`}
            rows={6}
          />
        </div>

        {/* Step 4: Notes */}
        <div className="border-t pt-6">
          <h2 className="text-lg font-medium text-gray-900 mb-4">Step 4: Notes (optional)</h2>
          <p className="text-sm text-gray-500 mb-2">
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
            <ul className="space-y-2">
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
        </div>

        {/* Create Button */}
        <div className="border-t pt-6 flex justify-end gap-3">
          <button
            onClick={() => navigate(`/organizations/${orgId}`)}
            className="px-4 py-2 text-gray-700 text-sm rounded-md border border-gray-300 hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            onClick={handleCreate}
            disabled={saving}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {saving ? 'Creating...' : 'Create Rule'}
          </button>
        </div>
      </div>
    </div>
  )
}
