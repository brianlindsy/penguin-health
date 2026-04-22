import { useState, useEffect, useMemo } from 'react'
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
  const [categoryFilter, setCategoryFilter] = useState('all')
  // null = default (rule_id asc). 'asc' / 'desc' when the user has clicked
  // the Category header to sort by that column.
  const [categorySort, setCategorySort] = useState(null)

  useEffect(() => {
    api.listRules(orgId)
      .then(data => setRules(Array.isArray(data) ? data : data?.rules || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId])

  // Distinct categories actually present on the rules, for the filter dropdown.
  const availableCategories = useMemo(() => {
    const set = new Set()
    rules.forEach(r => { if (r.category) set.add(r.category) })
    return Array.from(set).sort()
  }, [rules])

  const displayedRules = useMemo(() => {
    const filtered = categoryFilter === 'all'
      ? rules
      : rules.filter(r => r.category === categoryFilter)

    const list = [...filtered]
    if (categorySort) {
      list.sort((a, b) => {
        const cmp = (a.category || '').localeCompare(b.category || '')
        return categorySort === 'asc' ? cmp : -cmp
      })
    } else {
      list.sort((a, b) => {
        const aNum = parseInt(a.rule_id) || 0
        const bNum = parseInt(b.rule_id) || 0
        return aNum - bNum
      })
    }
    return list
  }, [rules, categoryFilter, categorySort])

  const toggleCategorySort = () => {
    setCategorySort(prev =>
      prev === 'asc' ? 'desc' : prev === 'desc' ? null : 'asc'
    )
  }

  const sortIndicator = categorySort === 'asc' ? '▲'
    : categorySort === 'desc' ? '▼'
    : null

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="mb-6">
          <h1 className="text-2xl font-semibold text-gray-900">Audit Rules</h1>
          <p className="text-sm text-gray-500 mt-1">
            {displayedRules.length}
            {displayedRules.length !== rules.length ? ` of ${rules.length}` : ''}
            {' '}{displayedRules.length === 1 ? 'rule' : 'rules'}
            {categoryFilter !== 'all' ? ` in ${categoryFilter}` : ' configured for this organization'}
          </p>
        </div>

        {/* Category filter */}
        {availableCategories.length > 0 && (
          <div className="flex items-center gap-3 mb-4">
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">Category</label>
            <select
              value={categoryFilter}
              onChange={(e) => setCategoryFilter(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All categories</option>
              {availableCategories.map(c => (
                <option key={c} value={c}>{c}</option>
              ))}
            </select>
            {categoryFilter !== 'all' && (
              <button
                onClick={() => setCategoryFilter('all')}
                className="text-sm text-blue-600 hover:text-blue-800"
              >
                Clear
              </button>
            )}
          </div>
        )}

        {error && <p className="text-red-600 mb-4">Error: {error}</p>}
        {loading ? (
          <p className="text-gray-500">Loading rules...</p>
        ) : rules.length === 0 ? (
          <p className="text-gray-500">No audit rules configured.</p>
        ) : displayedRules.length === 0 ? (
          <p className="text-gray-500">No rules in this category.</p>
        ) : (
          <div className="bg-white shadow rounded-lg overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-16">ID</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                    <button
                      onClick={toggleCategorySort}
                      className="inline-flex items-center gap-1 uppercase tracking-wide text-xs font-medium text-gray-500 hover:text-gray-700"
                      title="Click to sort by category"
                    >
                      Category
                      <span className="text-gray-400 w-3 text-center">{sortIndicator || '⇅'}</span>
                    </button>
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-24">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {displayedRules.map(rule => (
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
