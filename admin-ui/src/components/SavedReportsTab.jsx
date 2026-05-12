import { useEffect, useState } from 'react'
import { api } from '../api/client.js'
import { ResultRenderer } from './ResultRenderer.jsx'

function formatDate(iso) {
  if (!iso) return ''
  try {
    return new Date(iso).toLocaleString()
  } catch {
    return iso
  }
}

function ReportViewer({ orgId, reportId, onClose, onDeleted }) {
  const [report, setReport] = useState(null)
  const [error, setError] = useState('')
  const [deleting, setDeleting] = useState(false)

  useEffect(() => {
    let cancelled = false
    api.getReport(orgId, reportId)
      .then(data => { if (!cancelled) setReport(data) })
      .catch(err => { if (!cancelled) setError(err.message) })
    return () => { cancelled = true }
  }, [orgId, reportId])

  async function handleDelete() {
    if (!window.confirm('Delete this saved report?')) return
    setDeleting(true)
    try {
      await api.deleteReport(orgId, reportId)
      onDeleted?.()
      onClose?.()
    } catch (err) {
      setError(err.message)
      setDeleting(false)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/40 z-40 flex items-start justify-center overflow-y-auto p-6">
      <div className="bg-white rounded-lg shadow-xl max-w-6xl w-full p-6 my-6">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h2 className="text-xl font-semibold text-gray-900">
              {report?.name || 'Loading…'}
            </h2>
            {report && (
              <p className="text-xs text-gray-500 mt-1">
                Saved {formatDate(report.created_at)} by {report.created_by}
                {' · '}data may be stale
              </p>
            )}
          </div>
          <div className="flex gap-2">
            {report && (
              <button
                onClick={handleDelete}
                disabled={deleting}
                className="text-sm px-3 py-1.5 bg-red-50 text-red-700 rounded hover:bg-red-100 disabled:opacity-50"
              >
                {deleting ? 'Deleting…' : 'Delete'}
              </button>
            )}
            <button
              onClick={onClose}
              className="text-sm px-3 py-1.5 bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
            >
              Close
            </button>
          </div>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 rounded p-3 mb-4 text-sm text-red-800">
            {error}
          </div>
        )}

        {report && (
          <div className="space-y-3">
            <p className="text-sm text-gray-700">
              <strong>Question:</strong> {report.question}
            </p>
            {report.explanation && (
              <p className="text-sm text-gray-700">{report.explanation}</p>
            )}
            <details className="text-sm">
              <summary className="cursor-pointer text-gray-600 hover:text-gray-800">
                Show generated SQL
              </summary>
              <pre className="mt-2 bg-gray-50 border border-gray-200 rounded p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap">
                {report.sql}
              </pre>
            </details>
            <ResultRenderer
              viz_type={report.viz_type}
              columns={report.columns}
              rows={report.rows}
            />
            <div className="text-xs text-gray-500">
              {report.row_count} {report.row_count === 1 ? 'row' : 'rows'}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export function SavedReportsTab({ orgId, refreshKey = 0 }) {
  const [reports, setReports] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [activeId, setActiveId] = useState(null)
  // Local bump used by inner callbacks (delete, etc.) to trigger a refetch
  // without exposing an imperative reload function.
  const [innerKey, setInnerKey] = useState(0)

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
  }, [orgId, refreshKey, innerKey])

  const reload = () => {
    setLoading(true)
    setInnerKey(k => k + 1)
  }

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
                <tr
                  key={r.report_id}
                  onClick={() => setActiveId(r.report_id)}
                  className="hover:bg-gray-50 cursor-pointer"
                >
                  <td className="px-4 py-3 text-sm font-medium text-blue-700">
                    {r.name}
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

      {activeId && (
        <ReportViewer
          orgId={orgId}
          reportId={activeId}
          onClose={() => setActiveId(null)}
          onDeleted={reload}
        />
      )}
    </div>
  )
}
