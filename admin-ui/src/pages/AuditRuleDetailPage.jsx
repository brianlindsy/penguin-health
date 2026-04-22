import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'
import { StatusBadge } from '../components/StatusBadge.jsx'

// Customer-facing single-rule view. Read-only and intentionally minimal —
// hides Fields to Extract, the Notes section (input + list + Document ID +
// Validation Run ID), and the "Or edit the JSON directly" editor. Shows
// only what's meaningful for a reviewer: identity, category, description,
// status, type/version, and the rule text itself.
export function AuditRuleDetailPage() {
  const { orgId, ruleId } = useParams()

  const [rule, setRule] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  // Feedback form state. Uses the enhance-note endpoint under the hood to
  // attach the submitted comment as additional context on the rule. The user
  // experience stays "feedback only" — we don't surface existing notes here.
  const [feedbackText, setFeedbackText] = useState('')
  const [submittingFeedback, setSubmittingFeedback] = useState(false)
  const [feedbackMsg, setFeedbackMsg] = useState('')
  const [feedbackError, setFeedbackError] = useState('')

  useEffect(() => {
    api.getRule(orgId, ruleId)
      .then(data => setRule(data))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId, ruleId])

  const submitFeedback = async () => {
    const text = feedbackText.trim()
    if (!text || !rule) return
    setSubmittingFeedback(true)
    setFeedbackError('')
    setFeedbackMsg('')
    try {
      await api.enhanceNote(
        orgId,
        text,
        rule.rule_text || '',
        '', // document ID not collected in this view
        '', // validation run ID not collected in this view
        rule.rule_id,
        rule.notes || [],
      )
      setFeedbackText('')
      setFeedbackMsg('Thanks — your feedback was submitted.')
      setTimeout(() => setFeedbackMsg(''), 4000)
    } catch (err) {
      setFeedbackError(err.message || 'Failed to submit feedback.')
    } finally {
      setSubmittingFeedback(false)
    }
  }

  if (loading) {
    return (
      <OrgWorkspaceLayout>
        <p className="text-gray-500">Loading rule...</p>
      </OrgWorkspaceLayout>
    )
  }

  if (error) {
    return (
      <OrgWorkspaceLayout>
        <p className="text-red-600">Error: {error}</p>
      </OrgWorkspaceLayout>
    )
  }

  if (!rule) {
    return (
      <OrgWorkspaceLayout>
        <p className="text-gray-500">Rule not found.</p>
      </OrgWorkspaceLayout>
    )
  }

  return (
    <OrgWorkspaceLayout>
      <div className="max-w-3xl">
        <Link
          to={`/organizations/${orgId}/audit-rules`}
          className="text-sm text-blue-600 hover:text-blue-800 mb-3 inline-flex items-center gap-1"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
          </svg>
          Back to Audit Rules
        </Link>

        <div className="flex items-start justify-between mb-6 gap-4">
          <h1 className="text-2xl font-semibold text-gray-900">{rule.name}</h1>
          <StatusBadge enabled={rule.enabled} />
        </div>

        <div className="bg-white shadow rounded-lg p-6 space-y-6">
          <div className="grid grid-cols-2 gap-4">
            <DetailField label="Rule ID" value={rule.rule_id} mono />
            <DetailField label="Version" value={rule.version} />
            <DetailField label="Category" value={rule.category} />
            <DetailField label="Type" value={rule.type} />
          </div>

          {rule.description && (
            <DetailField label="Description" value={rule.description} />
          )}

          <div>
            <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">Rule Text</div>
            <div className="bg-gray-50 border border-gray-200 rounded-md p-3 text-sm text-gray-800 whitespace-pre-wrap">
              {rule.rule_text || <span className="text-gray-400 italic">No rule text.</span>}
            </div>
          </div>
        </div>

        {/* Feedback — submissions are attached to the rule as context notes
            so the team can tune the rule over time. */}
        <div className="bg-white shadow rounded-lg p-6 mt-6">
          <h2 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-1">
            Feedback
          </h2>
          <p className="text-xs text-gray-500 mb-3">
            See something off about this rule? Let us know how it should be interpreted or adjusted.
          </p>

          <textarea
            value={feedbackText}
            onChange={e => setFeedbackText(e.target.value)}
            rows={4}
            placeholder="Share your feedback on this rule..."
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />

          {feedbackError && (
            <p className="text-sm text-red-600 mt-2">{feedbackError}</p>
          )}

          <div className="flex items-center gap-3 mt-3">
            <button
              onClick={submitFeedback}
              disabled={submittingFeedback || !feedbackText.trim()}
              className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              {submittingFeedback ? 'Submitting...' : 'Submit Feedback'}
            </button>
            {feedbackMsg && (
              <span className="text-sm text-green-600">{feedbackMsg}</span>
            )}
          </div>
        </div>
      </div>
    </OrgWorkspaceLayout>
  )
}

function DetailField({ label, value, mono = false }) {
  return (
    <div>
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">{label}</div>
      <div className={`text-sm text-gray-900 ${mono ? 'font-mono' : ''}`}>
        {value || <span className="text-gray-400 italic">—</span>}
      </div>
    </div>
  )
}
