import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { ValidationStatusBadge } from '../components/ValidationStatusBadge.jsx'

export function ValidationRunDetailPage() {
  const { orgId, runId } = useParams()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [expandedDoc, setExpandedDoc] = useState(null)

  useEffect(() => {
    api.getValidationRun(orgId, runId)
      .then(setData)
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId, runId])

  if (loading) return <p className="text-gray-500">Loading validation run...</p>
  if (error) return <p className="text-red-600">Error: {error}</p>
  if (!data) return <p className="text-gray-500">Validation run not found</p>

  const getOverallStatus = (summary) => {
    if (summary?.failed > 0) return 'FAIL'
    if (summary?.skipped > 0 && summary?.passed === 0) return 'SKIP'
    return 'PASS'
  }

  return (
    <div>
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-2 text-sm text-gray-500 mb-2">
          <Link to={`/organizations/${orgId}`} className="hover:text-blue-600">
            Organization
          </Link>
          <span>/</span>
          <span>Validation Results</span>
          <span>/</span>
          <span className="font-mono">{runId}</span>
        </div>
        <h1 className="text-2xl font-semibold text-gray-900">
          Validation Run: {runId}
        </h1>
        <p className="text-sm text-gray-500 mt-1">
          {data.total_count} document{data.total_count !== 1 ? 's' : ''} validated
        </p>
      </div>

      {/* Results Table */}
      {data.documents.length === 0 ? (
        <p className="text-gray-500">No documents found in this validation run.</p>
      ) : (
        <div className="bg-white shadow rounded-lg overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Document ID</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Timestamp</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Pass</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Fail</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-20">Skip</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {data.documents.map(doc => (
                <>
                  <tr
                    key={doc.document_id}
                    className="hover:bg-gray-50 cursor-pointer"
                    onClick={() => setExpandedDoc(expandedDoc === doc.document_id ? null : doc.document_id)}
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <span className="text-gray-400">
                          {expandedDoc === doc.document_id ? '▼' : '▶'}
                        </span>
                        <span className="text-sm font-mono text-gray-900">{doc.document_id}</span>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600">
                      {doc.validation_timestamp
                        ? new Date(doc.validation_timestamp).toLocaleString()
                        : '-'}
                    </td>
                    <td className="px-4 py-3 text-sm text-green-600 font-medium">
                      {doc.summary?.passed ?? 0}
                    </td>
                    <td className="px-4 py-3 text-sm text-red-600 font-medium">
                      {doc.summary?.failed ?? 0}
                    </td>
                    <td className="px-4 py-3 text-sm text-yellow-600 font-medium">
                      {doc.summary?.skipped ?? 0}
                    </td>
                    <td className="px-4 py-3">
                      <ValidationStatusBadge status={getOverallStatus(doc.summary)} />
                    </td>
                  </tr>
                  {expandedDoc === doc.document_id && (
                    <tr key={`${doc.document_id}-rules`}>
                      <td colSpan={6} className="px-4 py-4 bg-gray-50">
                        <RuleResultsPanel rules={doc.rules} />
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}


function RuleResultsPanel({ rules }) {
  if (!rules || rules.length === 0) {
    return <p className="text-sm text-gray-500">No rule results available.</p>
  }

  // Extract reasoning from message (format: "STATUS - reasoning")
  const extractReasoning = (rule) => {
    const message = rule.message || ''
    const status = rule.status || ''
    if (message.startsWith(`${status} - `)) {
      return message.substring(status.length + 3)
    }
    if (message.startsWith(`${status}: `)) {
      return message.substring(status.length + 2)
    }
    return message || '-'
  }

  return (
    <div>
      <h4 className="text-sm font-medium text-gray-700 mb-3">Rule Results</h4>
      <div className="bg-white rounded border">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-100">
            <tr>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Rule</th>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase w-20">Status</th>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Reasoning</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {rules.map((rule, idx) => (
              <tr key={rule.rule_id || idx}>
                <td className="px-3 py-2 text-sm text-gray-900">{rule.rule_name || rule.rule_id}</td>
                <td className="px-3 py-2 text-sm text-gray-600">{rule.category || '-'}</td>
                <td className="px-3 py-2">
                  <ValidationStatusBadge status={rule.status} />
                </td>
                <td className="px-3 py-2 text-sm text-gray-600 max-w-md">
                  {extractReasoning(rule)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
