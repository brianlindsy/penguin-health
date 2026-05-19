import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client.js'

function formatDate(iso) {
  if (!iso) return ''
  try {
    return new Date(iso).toLocaleString()
  } catch {
    return iso
  }
}

export function SavedReportsTab({ orgId, refreshKey = 0 }) {
  const [reports, setReports] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    api.listReports(orgId)
      .then(data => {
        if (cancelled) return
        setReports(data.reports || [])
        setError('')
      })
      .catch(err => {
        if (!cancelled) setError(err.message)
      })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [orgId, refreshKey])

  if (loading) {
    return <p className="text-sm text-gray-500">Loading reports…</p>
  }

  return (
    <div>
      {error && (
        <div className="bg-red-50 border border-red-200 rounded p-3 mb-4 text-sm text-red-800">
          {error}
        </div>
      )}

      {reports.length === 0 ? (
        <div className="bg-white shadow rounded-lg p-6 text-center">
          <p className="text-sm text-gray-600">
            No saved reports yet. Run a query in the <strong>NL Explorer</strong>{' '}
            tab and click <strong>Save as report</strong> to keep it.
          </p>
        </div>
      ) : (
        <div className="bg-white shadow rounded-lg overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Saved</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">By</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Mode</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Viz</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Rows</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {reports.map(r => (
                <tr key={r.report_id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm font-medium">
                    <Link
                      to={`/organizations/${orgId}/analytics/reports/${r.report_id}`}
                      className="text-blue-700 hover:underline"
                    >
                      {r.name}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {formatDate(r.created_at)}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">{r.created_by}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{r.mode}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{r.viz_type}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{r.row_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
