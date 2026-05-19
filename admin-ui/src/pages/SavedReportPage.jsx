import { useEffect, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { usePermissions } from '../auth/usePermissions.js'
import { ResultRenderer } from '../components/ResultRenderer.jsx'

function formatDate(iso) {
  if (!iso) return ''
  try {
    return new Date(iso).toLocaleString()
  } catch {
    return iso
  }
}

export function SavedReportPage() {
  const { orgId, reportId } = useParams()
  const navigate = useNavigate()
  const { isSuperAdmin } = usePermissions()

  const [report, setReport] = useState(null)
  const [loadError, setLoadError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [deleting, setDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState('')
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    let cancelled = false
    api.getReport(orgId, reportId)
      .then(data => {
        if (cancelled) return
        setReport(data)
        setLoadError(null)
      })
      .catch(err => { if (!cancelled) setLoadError(err) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [orgId, reportId])

  async function handleDelete() {
    if (!window.confirm('Delete this saved report?')) return
    setDeleting(true)
    setDeleteError('')
    try {
      await api.deleteReport(orgId, reportId)
      navigate(`/organizations/${orgId}/analytics?tab=reports`)
    } catch (err) {
      setDeleteError(err.message)
      setDeleting(false)
    }
  }

  async function handleCopyLink() {
    const url = window.location.href
    try {
      await navigator.clipboard.writeText(url)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      window.prompt('Copy this link:', url)
    }
  }

  const backLink = `/organizations/${orgId}/analytics?tab=reports`

  if (loading) {
    return (
      <div className="max-w-6xl mx-auto px-4 py-6">
        <p className="text-sm text-gray-500">Loading report…</p>
      </div>
    )
  }

  if (loadError) {
    const status = loadError.status
    let title = 'Unable to load report'
    let body = loadError.message || 'Something went wrong.'
    if (status === 403) {
      title = 'You don’t have access to this report'
      body = 'This report belongs to a different organization. Ask a super admin or a member of that organization to share it with you.'
    } else if (status === 404) {
      title = 'Report not found'
      body = 'This report no longer exists. It may have been deleted.'
    }
    return (
      <div className="max-w-3xl mx-auto px-4 py-12">
        <div className="bg-white shadow rounded-lg p-8 text-center">
          <h1 className="text-xl font-semibold text-gray-900 mb-2">{title}</h1>
          <p className="text-sm text-gray-600 mb-6">{body}</p>
          <Link
            to={backLink}
            className="text-sm text-blue-600 hover:underline"
          >
            Back to Saved Reports
          </Link>
        </div>
      </div>
    )
  }

  if (!report) return null

  const isRedacted = Boolean(report.redacted)
  const vizType = isRedacted ? 'table' : report.viz_type

  return (
    <div className="max-w-6xl mx-auto px-4 py-6">
      <div className="mb-4">
        <Link
          to={backLink}
          className="text-sm text-blue-600 hover:underline"
        >
          &larr; Back to Saved Reports
        </Link>
      </div>

      <div className="flex items-start justify-between mb-4">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">{report.name}</h1>
          <p className="text-xs text-gray-500 mt-1">
            Saved {formatDate(report.created_at)} by {report.created_by}
            {' · data may be stale'}
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleCopyLink}
            className="text-sm px-3 py-1.5 bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
          >
            {copied ? 'Copied!' : 'Copy link'}
          </button>
          {isSuperAdmin && (
            <button
              onClick={handleDelete}
              disabled={deleting}
              className="text-sm px-3 py-1.5 bg-red-50 text-red-700 rounded hover:bg-red-100 disabled:opacity-50"
            >
              {deleting ? 'Deleting…' : 'Delete'}
            </button>
          )}
        </div>
      </div>

      {deleteError && (
        <div className="bg-red-50 border border-red-200 rounded p-3 mb-4 text-sm text-red-800">
          {deleteError}
        </div>
      )}

      <div className="space-y-3">
        <p className="text-sm text-gray-700">
          <strong>Question:</strong> {report.question}
        </p>
        {report.explanation && (
          <p className="text-sm text-gray-700">{report.explanation}</p>
        )}
        {!isRedacted && report.sql && (
          <details className="text-sm">
            <summary className="cursor-pointer text-gray-600 hover:text-gray-800">
              Show generated SQL
            </summary>
            <pre className="mt-2 bg-gray-50 border border-gray-200 rounded p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap">
              {report.sql}
            </pre>
          </details>
        )}
        <ResultRenderer
          viz_type={vizType}
          columns={report.columns}
          rows={report.rows}
        />
        <div className="text-xs text-gray-500">
          {report.row_count} {report.row_count === 1 ? 'row' : 'rows'}
        </div>
      </div>
    </div>
  )
}
