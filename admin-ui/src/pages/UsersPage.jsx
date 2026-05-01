import { useState, useEffect, useMemo } from 'react'
import { useParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'
import { CATEGORIES, ANALYTICS_PAGES } from '../auth/usePermissions.js'

// Super-admin-only screen for managing user permissions within an organization.
// The route is gated by <RoleGuard requireSuperAdmin> in App.jsx; this component
// itself trusts the gate and just renders the management UI.
export function UsersPage() {
  const { orgId } = useParams()
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [editing, setEditing] = useState(null) // { email, role, report_permissions, analytics_permissions, isNew }
  const [savingMsg, setSavingMsg] = useState('')

  const loadUsers = () => {
    setLoading(true)
    setError('')
    api.listOrgUsers(orgId)
      .then(data => setUsers(data.users || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    loadUsers()
  }, [orgId])

  const startCreate = () => {
    setEditing({
      email: '',
      role: 'member',
      report_permissions: blankReportPermissions(),
      analytics_permissions: [],
      isNew: true,
    })
  }

  const startEdit = (user) => {
    setEditing({
      email: user.email,
      role: user.role || 'member',
      report_permissions: mergeReportPermissions(user.report_permissions),
      analytics_permissions: user.analytics_permissions || [],
      isNew: false,
    })
  }

  const closeEditor = () => {
    setEditing(null)
    setSavingMsg('')
  }

  const handleSave = async (draft) => {
    setSavingMsg('')
    try {
      await api.upsertOrgUser(orgId, draft.email, {
        role: draft.role,
        report_permissions: draft.report_permissions,
        analytics_permissions: draft.analytics_permissions,
      })
      closeEditor()
      loadUsers()
    } catch (err) {
      setSavingMsg(`Error: ${err.message}`)
    }
  }

  const handleDelete = async (email) => {
    const ok = window.confirm(
      `Remove all permissions for ${email} in this organization? They will lose access immediately.`,
    )
    if (!ok) return
    try {
      await api.deleteOrgUser(orgId, email)
      loadUsers()
    } catch (err) {
      setError(err.message)
    }
  }

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="flex items-center justify-between mb-5">
          <div>
            <h1 className="text-2xl font-semibold text-gray-900">Users & Permissions</h1>
            <p className="text-sm text-gray-500 mt-1">
              Manage which users can view and run reports in this organization.
            </p>
          </div>
          <button
            onClick={startCreate}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
          >
            Add User
          </button>
        </div>

        {error && <p className="text-red-600 mb-4">Error: {error}</p>}

        {loading ? (
          <p className="text-gray-500">Loading users...</p>
        ) : users.length === 0 ? (
          <p className="text-gray-500">No users have been granted permissions in this organization yet.</p>
        ) : (
          <div className="bg-white shadow rounded-lg overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Email</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase w-32">Role</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Report access</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Analytics</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase w-32">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {users.map(user => (
                  <tr key={user.email} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm text-gray-900">{user.email}</td>
                    <td className="px-4 py-3"><RoleBadge role={user.role} /></td>
                    <td className="px-4 py-3 text-sm text-gray-600">
                      <ReportSummary perms={user.report_permissions} role={user.role} />
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600">
                      <AnalyticsSummary pages={user.analytics_permissions} role={user.role} />
                    </td>
                    <td className="px-4 py-3 text-right">
                      <button
                        onClick={() => startEdit(user)}
                        className="text-sm text-blue-600 hover:text-blue-800 mr-3"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => handleDelete(user.email)}
                        className="text-sm text-red-600 hover:text-red-800"
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {editing && (
          <UserEditor
            draft={editing}
            onSave={handleSave}
            onCancel={closeEditor}
            errorMsg={savingMsg}
          />
        )}
      </div>
    </OrgWorkspaceLayout>
  )
}

function UserEditor({ draft, onSave, onCancel, errorMsg }) {
  const [email, setEmail] = useState(draft.email)
  const [role, setRole] = useState(draft.role)
  const [reportPerms, setReportPerms] = useState(draft.report_permissions)
  const [analyticsPerms, setAnalyticsPerms] = useState(draft.analytics_permissions)
  const isOrgAdmin = role === 'org_admin'

  const toggleVerb = (category, verb) => {
    setReportPerms(prev => {
      const verbs = new Set(prev[category] || [])
      if (verbs.has(verb)) verbs.delete(verb)
      else verbs.add(verb)
      return { ...prev, [category]: Array.from(verbs) }
    })
  }

  const toggleAnalytics = (page) => {
    setAnalyticsPerms(prev =>
      prev.includes(page) ? prev.filter(p => p !== page) : [...prev, page],
    )
  }

  const canSave = useMemo(() => {
    if (!email.trim()) return false
    if (!email.includes('@')) return false
    return true
  }, [email])

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center p-4 z-20"
         role="dialog" aria-modal="true">
      <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        <div className="p-5 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-900">
            {draft.isNew ? 'Add User' : `Edit ${draft.email}`}
          </h2>
          <button onClick={onCancel} className="text-gray-400 hover:text-gray-600">
            ✕
          </button>
        </div>

        <div className="p-5 space-y-5">
          {draft.isNew && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="user@example.com"
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm"
              />
              <p className="text-xs text-gray-500 mt-1">
                The user must already exist in Cognito. This page only assigns permissions.
              </p>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Role</label>
            <select
              value={role}
              onChange={(e) => setRole(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-md text-sm"
            >
              <option value="member">Member — only the permissions checked below</option>
              <option value="org_admin">Org Admin — full access in this organization</option>
            </select>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="block text-sm font-medium text-gray-700">Report access</label>
              {isOrgAdmin && (
                <span className="text-xs text-gray-500 italic">Org admins have full access</span>
              )}
            </div>
            <div className={`border border-gray-200 rounded-md ${isOrgAdmin ? 'opacity-50' : ''}`}>
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
                    <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 uppercase w-20">View</th>
                    <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 uppercase w-20">Run</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {CATEGORIES.map(cat => {
                    const verbs = reportPerms[cat] || []
                    return (
                      <tr key={cat}>
                        <td className="px-3 py-2 text-gray-700">{cat}</td>
                        <td className="px-3 py-2 text-center">
                          <input
                            type="checkbox"
                            checked={verbs.includes('view')}
                            disabled={isOrgAdmin}
                            onChange={() => toggleVerb(cat, 'view')}
                            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                            aria-label={`${cat} view`}
                          />
                        </td>
                        <td className="px-3 py-2 text-center">
                          <input
                            type="checkbox"
                            checked={verbs.includes('run')}
                            disabled={isOrgAdmin}
                            onChange={() => toggleVerb(cat, 'run')}
                            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                            aria-label={`${cat} run`}
                          />
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
            <p className="text-xs text-gray-500 mt-1">
              View and Run are independent: a user with only "Run" can trigger
              validations but not see the results.
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Analytics pages</label>
            <div className="space-y-1.5">
              {ANALYTICS_PAGES.map(page => (
                <label key={page} className={`flex items-center gap-2 text-sm ${isOrgAdmin ? 'opacity-50' : ''}`}>
                  <input
                    type="checkbox"
                    checked={analyticsPerms.includes(page)}
                    disabled={isOrgAdmin}
                    onChange={() => toggleAnalytics(page)}
                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="text-gray-700">{ANALYTICS_PAGE_LABELS[page] || page}</span>
                </label>
              ))}
            </div>
          </div>

          {errorMsg && <p className="text-sm text-red-600">{errorMsg}</p>}
        </div>

        <div className="p-5 border-t border-gray-200 flex items-center justify-end gap-2">
          <button
            onClick={onCancel}
            className="px-4 py-2 text-sm text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            onClick={() => onSave({
              email: email.trim(),
              role,
              report_permissions: reportPerms,
              analytics_permissions: analyticsPerms,
            })}
            disabled={!canSave}
            className="px-4 py-2 text-sm text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            Save
          </button>
        </div>
      </div>
    </div>
  )
}

function RoleBadge({ role }) {
  if (role === 'org_admin') {
    return (
      <span className="px-2 py-0.5 text-xs font-medium bg-purple-100 text-purple-800 rounded-full">
        Org Admin
      </span>
    )
  }
  return (
    <span className="px-2 py-0.5 text-xs font-medium bg-gray-100 text-gray-700 rounded-full">
      Member
    </span>
  )
}

function ReportSummary({ perms, role }) {
  if (role === 'org_admin') return <span className="italic text-gray-500">Full access</span>
  const entries = Object.entries(perms || {})
    .map(([cat, verbs]) => [cat, (verbs || []).filter(v => v === 'view' || v === 'run')])
    .filter(([, verbs]) => verbs.length > 0)
  if (entries.length === 0) return <span className="italic text-gray-400">None</span>
  return (
    <div className="flex flex-wrap gap-1">
      {entries.map(([cat, verbs]) => (
        <span key={cat} className="text-xs bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded">
          {cat}: {verbs.join('+')}
        </span>
      ))}
    </div>
  )
}

function AnalyticsSummary({ pages, role }) {
  if (role === 'org_admin') return <span className="italic text-gray-500">Full access</span>
  const list = pages || []
  if (list.length === 0) return <span className="italic text-gray-400">None</span>
  return (
    <div className="flex flex-wrap gap-1">
      {list.map(p => (
        <span key={p} className="text-xs bg-green-50 text-green-700 px-1.5 py-0.5 rounded">
          {ANALYTICS_PAGE_LABELS[p] || p}
        </span>
      ))}
    </div>
  )
}

const ANALYTICS_PAGE_LABELS = {
  staff_performance: 'Staff Performance',
  revenue_analysis: 'Revenue Analysis',
}

function blankReportPermissions() {
  return CATEGORIES.reduce((acc, cat) => {
    acc[cat] = []
    return acc
  }, {})
}

function mergeReportPermissions(existing) {
  const merged = blankReportPermissions()
  for (const [cat, verbs] of Object.entries(existing || {})) {
    if (cat in merged) merged[cat] = [...(verbs || [])]
  }
  return merged
}
