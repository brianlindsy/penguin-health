import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'

// Super-admin-only — the route is gated by <RoleGuard requireSuperAdmin>
// in App.jsx; this component trusts the gate and just renders the table.

const EVENT_LABELS = {
  validation_run_complete: 'Validation run complete',
  eligibility_issue: 'Eligibility issue',
}

export function NotificationPreferencesPage() {
  const { orgId } = useParams()
  const [users, setUsers] = useState(null)
  const [eventTypes, setEventTypes] = useState([])
  const [error, setError] = useState(null)
  const [savingKey, setSavingKey] = useState(null)

  useEffect(() => {
    if (!orgId) return
    let cancelled = false
    api
      .listOrgSubscriptions(orgId)
      .then((data) => {
        if (cancelled) return
        setUsers(data?.users || [])
        setEventTypes(data?.event_types || [])
      })
      .catch((e) => {
        if (cancelled) return
        setError(e.message || 'Failed to load subscriptions')
      })
    return () => {
      cancelled = true
    }
  }, [orgId])

  async function handleToggle(email, eventType, nextEnabled) {
    const key = `${email}::${eventType}`
    setSavingKey(key)
    setError(null)
    try {
      await api.setOrgUserSubscription(orgId, email, eventType, nextEnabled)
      // Optimistically update local state; an updated_at would require
      // re-reading the row, so we leave that field stale until next load.
      setUsers((prev) =>
        prev.map((u) =>
          u.email !== email
            ? u
            : {
                ...u,
                subscriptions: u.subscriptions.map((s) =>
                  s.event_type === eventType ? { ...s, enabled: nextEnabled } : s,
                ),
              },
        ),
      )
    } catch (e) {
      setError(e.message || 'Failed to update subscription')
    } finally {
      setSavingKey(null)
    }
  }

  if (users === null && !error) {
    return (
      <OrgWorkspaceLayout>
        <p className="text-sm text-gray-500">Loading...</p>
      </OrgWorkspaceLayout>
    )
  }

  return (
    <OrgWorkspaceLayout>
      <div>
        <div className="mb-5">
          <h1 className="text-2xl font-semibold text-gray-900">Notifications</h1>
          <p className="mt-1 text-sm text-gray-600">
            Choose which users in this organization receive email for each event.
            Emails contain only timestamps, aggregate counts, and a deep link &mdash;
            never patient identifiers.
          </p>
        </div>

        {error && (
          <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
            {error}
          </div>
        )}

        {users.length === 0 ? (
          <p className="text-gray-500">
            No users have been granted permissions in this organization yet.
            Add users on the Users &amp; Permissions page first.
          </p>
        ) : (
          <div className="bg-white shadow rounded-lg overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                    User
                  </th>
                  {eventTypes.map((et) => (
                    <th
                      key={et}
                      className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase w-48"
                    >
                      {EVENT_LABELS[et] || et}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {users.map((user) => (
                  <tr key={user.email} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm text-gray-900">{user.email}</td>
                    {eventTypes.map((et) => {
                      const sub = user.subscriptions.find((s) => s.event_type === et)
                      const enabled = !!sub?.enabled
                      const key = `${user.email}::${et}`
                      const isSaving = savingKey === key
                      return (
                        <td key={et} className="px-4 py-3 text-center">
                          <input
                            type="checkbox"
                            checked={enabled}
                            disabled={isSaving}
                            onChange={(e) => handleToggle(user.email, et, e.target.checked)}
                            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                            aria-label={`${user.email} ${EVENT_LABELS[et] || et}`}
                          />
                        </td>
                      )
                    })}
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
