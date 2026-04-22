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

  useEffect(() => {
    api.getRule(orgId, ruleId)
      .then(data => setRule(data))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId, ruleId])

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

        {/* Feedback — the accumulated enhance-notes that guide how this
            rule is interpreted. Read-only; users see the contextual guidance
            but can't edit it from this view. */}
        <div className="bg-white shadow rounded-lg p-6 mt-6">
          <h2 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-1">
            Feedback
          </h2>
          <p className="text-xs text-gray-500 mb-3">
            Context and clarifications attached to this rule to guide its interpretation.
          </p>

          {rule.notes && rule.notes.length > 0 ? (
            <ul className="space-y-2">
              {rule.notes.map((note, index) => (
                <li
                  key={index}
                  className="text-sm text-gray-700 bg-gray-50 border border-gray-200 rounded-md p-3"
                >
                  {note}
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-gray-400 italic">No feedback recorded for this rule.</p>
          )}
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
