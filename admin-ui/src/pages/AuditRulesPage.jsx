import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'
import { StatusBadge } from '../components/StatusBadge.jsx'

// Customer-facing Audit Rules list. Lives inside the workspace sidebar so
// the user never loses the dashboard nav. Read-only: no "Add Rule" action,
// no quick-toggle. Click a row to view the rule's details on the next page.
export function AuditRulesPage() {
  const { orgId } = useParams()
  const [rules, setRules] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    api.listRules(orgId)
      .then(data => setRules(Array.isArray(data) ? data : data?.rules || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  const sorted = [...rules].sort((a, b) => {
    const aNum = parseInt(a.rule_id) || 0
    const bNum = parseInt(b.rule_id) || 0
    return aNum - bNum
  })

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="mb-6">
          <h1 className="text-2xl font-semibold text-gray-900">Audit Rules</h1>
          <p className="text-sm text-gray-500 mt-1">
            {rules.length} {rules.length === 1 ? 'rule' : 'rules'} configured for this organization
          </p>
        </div>

        {error && <p className="text-red-600 mb-4">Error: {error}</p>}
        {loading ? (
          <p className="text-gray-500">Loading rules...</p>
        ) : rules.length === 0 ? (
          <p className="text-gray-500">No audit rules configured.</p>
        ) : (
          <div className="bg-white shadow rounded-lg overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-16">ID</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Type</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {sorted.map(rule => (
                  <tr key={rule.rule_id} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm font-mono text-gray-600">{rule.rule_id}</td>
                    <td className="px-4 py-3">
                      <Link
                        to={`/organizations/${orgId}/audit-rules/${rule.rule_id}`}
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
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </OrgWorkspaceLayout>
  )
}
