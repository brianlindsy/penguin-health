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
  // Unified sort control for the table. Click a header to cycle:
  //   other column       → asc on this column
  //   asc on this column → desc on this column
  //   desc on this column → back to default (ID asc)
  const [sort, setSort] = useState({ column: 'id', direction: 'asc' })

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
    list.sort((a, b) => {
      if (sort.column === 'category') {
        const cmp = (a.category || '').localeCompare(b.category || '')
        return sort.direction === 'asc' ? cmp : -cmp
      }
      // Default / 'id' column — numeric sort on the rule id.
      const aNum = parseInt(a.rule_id) || 0
      const bNum = parseInt(b.rule_id) || 0
      return sort.direction === 'asc' ? aNum - bNum : bNum - aNum
    })
    return list
  }, [rules, categoryFilter, sort])

  const toggleSort = (column) => {
    setSort(prev => {
      if (prev.column !== column) return { column, direction: 'asc' }
      if (prev.direction === 'asc') return { column, direction: 'desc' }
      // desc → back to default
      return { column: 'id', direction: 'asc' }
    })
  }

  const sortIndicator = (column) => {
    if (sort.column !== column) return '⇅'
    return sort.direction === 'asc' ? '▲' : '▼'
  }

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="mb-5">
          <h1 className="text-2xl font-semibold text-gray-900">Audit Rules</h1>
          <p className="text-sm text-gray-500 mt-1">
            {displayedRules.length}
            {displayedRules.length !== rules.length ? ` of ${rules.length}` : ''}
            {' '}{displayedRules.length === 1 ? 'rule' : 'rules'}
            {categoryFilter !== 'all' ? ` in ${categoryFilter}` : ' configured for this organization'}
          </p>
        </div>

        {/* Filter bar — compact pill controls, matching the Staff Performance
            landing page. Category is the only filter for now. */}
        {availableCategories.length > 0 && (
          <div className="flex items-center gap-2 mb-4 flex-wrap">
            <span className="inline-flex items-center gap-1.5 text-xs font-medium text-gray-400 uppercase tracking-wide">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
              </svg>
              Filters
            </span>

            <div className="inline-flex items-center gap-1.5 bg-white border border-gray-200 rounded-full pl-3 pr-1 py-0.5 shadow-sm">
              <svg className="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 7h.01M7 3h5c.512 0 1.024.195 1.414.586l7 7a2 2 0 010 2.828l-7 7a2 2 0 01-2.828 0l-7-7A1.994 1.994 0 013 12V7a4 4 0 014-4z" />
              </svg>
              <select
                value={categoryFilter}
                onChange={(e) => setCategoryFilter(e.target.value)}
                className="text-xs font-medium text-gray-700 bg-transparent border-0 focus:outline-none focus:ring-0 pr-1 py-1 cursor-pointer"
              >
                <option value="all">All categories</option>
                {availableCategories.map(c => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>

            {categoryFilter !== 'all' && (
              <button
                onClick={() => setCategoryFilter('all')}
                className="text-xs text-blue-600 hover:text-blue-800"
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
                  <th className="px-4 py-3 text-left w-16">
                    <button
                      onClick={() => toggleSort('id')}
                      className="inline-flex items-center gap-1 uppercase tracking-wide text-xs font-medium text-gray-500 hover:text-gray-700"
                      title="Click to sort by rule number"
                    >
                      ID
                      <span className="text-gray-400 w-3 text-center">{sortIndicator('id')}</span>
                    </button>
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                  <th className="px-4 py-3 text-left">
                    <button
                      onClick={() => toggleSort('category')}
                      className="inline-flex items-center gap-1 uppercase tracking-wide text-xs font-medium text-gray-500 hover:text-gray-700"
                      title="Click to sort by category"
                    >
                      Category
                      <span className="text-gray-400 w-3 text-center">{sortIndicator('category')}</span>
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
