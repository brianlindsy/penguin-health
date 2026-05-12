import { useEffect, useRef, useState } from 'react'
import { api, ApiError } from '../api/client.js'
import { ResultRenderer } from './ResultRenderer.jsx'

// Poll cadence for deep-analysis job status. 1.5s keeps the live-progress
// UI responsive without hammering the API for long jobs.
const DEEP_JOB_POLL_INTERVAL_MS = 1500
// Stop polling after this long even if the server keeps reporting running —
// guards against a runaway worker hanging the UI. Matches worker Lambda
// timeout (10 min) with a bit of headroom.
const DEEP_JOB_POLL_TIMEOUT_MS = 11 * 60 * 1000

const EXAMPLE_QUESTIONS = [
  'How many visits per program in March 2025?',
  'Weekly chart count trend over the last 3 months',
  'Top 10 employees by chart count last month',
  'Rules with the most failures in the last 30 days',
  'Where did referrals come from last quarter?',
]

const ERROR_MESSAGES = {
  NOT_SELECT: 'Only SELECT queries are allowed. Rephrase your question.',
  FORBIDDEN_KEYWORD:
    'The generated query used a disallowed keyword (INSERT/UPDATE/DELETE/DROP/etc). Try rephrasing.',
  MULTIPLE_STATEMENTS: 'Only one SQL statement is allowed per question.',
  DISALLOWED_TABLE:
    "The generated query referenced a table outside this org's analytics dataset.",
  NO_TABLE: 'The generated query did not reference any analytics table.',
  ORG_NOT_PROVISIONED: 'Analytics is not provisioned for this organization yet.',
  SCOPE_TOO_LARGE:
    'Too many rows match for deep analysis. Tighten the filters and try again.',
  REPORT_TOO_LARGE:
    'This result is too large to save. Reduce the row or column count and re-run.',
  ATHENA_ERROR: 'Athena rejected the generated query. See SQL below.',
  LLM_ERROR: 'The AI model call failed. Try again.',
  LLM_BAD_RESPONSE:
    "The AI model returned an unexpected response. Try rephrasing the question.",
}

function friendlyError(err) {
  if (err && err.code && ERROR_MESSAGES[err.code]) {
    return ERROR_MESSAGES[err.code]
  }
  return err?.message || 'Something went wrong.'
}

function isApproximate(explanation) {
  if (!explanation) return false
  return /\bapproximate\b/i.test(explanation)
}

function SqlDetails({ sql, label = 'Show generated SQL' }) {
  if (!sql) return null
  return (
    <details className="mb-3 text-sm">
      <summary className="cursor-pointer text-gray-600 hover:text-gray-800">
        {label}
      </summary>
      <pre className="mt-2 bg-gray-50 border border-gray-200 rounded p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap">
        {sql}
      </pre>
    </details>
  )
}

function SaveReportControl({ orgId, result, onSaved }) {
  const [open, setOpen] = useState(false)
  const [name, setName] = useState('')
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState('')

  if (!result) return null

  async function handleSave() {
    if (!name.trim()) return
    setSaving(true)
    setMsg('')
    try {
      await api.saveReport(orgId, {
        name: name.trim(),
        question: result.question,
        sql: result.sql,
        viz_type: result.viz_type,
        mode: result.mode,
        explanation: result.explanation || '',
        columns: result.columns,
        rows: result.rows,
        row_count: result.row_count,
      })
      setMsg('Saved')
      setOpen(false)
      setName('')
      onSaved?.()
      setTimeout(() => setMsg(''), 3000)
    } catch (err) {
      setMsg(`Error: ${friendlyError(err)}`)
    } finally {
      setSaving(false)
    }
  }

  if (!open) {
    return (
      <div className="flex items-center gap-3">
        <button
          onClick={() => setOpen(true)}
          className="text-sm px-3 py-1.5 bg-blue-600 text-white rounded hover:bg-blue-700"
        >
          Save as report
        </button>
        {msg && <span className="text-xs text-gray-600">{msg}</span>}
      </div>
    )
  }

  return (
    <div className="flex items-center gap-2">
      <input
        type="text"
        autoFocus
        placeholder="Report name"
        value={name}
        onChange={e => setName(e.target.value)}
        className="text-sm px-2 py-1 border border-gray-300 rounded"
      />
      <button
        onClick={handleSave}
        disabled={saving || !name.trim()}
        className="text-sm px-3 py-1.5 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
      >
        {saving ? 'Saving…' : 'Save'}
      </button>
      <button
        onClick={() => { setOpen(false); setName(''); setMsg('') }}
        className="text-sm px-3 py-1.5 bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
      >
        Cancel
      </button>
      {msg && <span className="text-xs text-gray-600">{msg}</span>}
    </div>
  )
}

function DeepConfirmCard({ pending, onConfirm, onCancel, running }) {
  // pending: { reason, scope_sql, estimated_rows, question }
  const estSeconds = Math.max(2, Math.round(pending.estimated_rows * 1.2))
  return (
    <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
      <h3 className="font-medium text-amber-900 mb-1">
        This question needs AI analysis of narrative text
      </h3>
      {pending.reason && (
        <p className="text-sm text-amber-800 mb-2">{pending.reason}</p>
      )}
      <p className="text-sm text-amber-800 mb-3">
        To answer, I&apos;ll read narrative fields from{' '}
        <strong>~{pending.estimated_rows}</strong> chart rows.
        Estimated time: <strong>~{estSeconds} seconds</strong>.
      </p>
      <SqlDetails sql={pending.scope_sql} label="Show scoping SQL" />
      <div className="flex items-center gap-2 mt-3">
        <button
          onClick={onConfirm}
          disabled={running}
          className="text-sm px-3 py-1.5 bg-amber-600 text-white rounded hover:bg-amber-700 disabled:opacity-50"
        >
          {running ? `Running deep analysis (~${estSeconds}s)…` : 'Run deep analysis'}
        </button>
        <button
          onClick={onCancel}
          disabled={running}
          className="text-sm px-3 py-1.5 bg-white border border-gray-300 text-gray-700 rounded hover:bg-gray-50 disabled:opacity-50"
        >
          Cancel
        </button>
      </div>
    </div>
  )
}

export function NLAnalyticsExplorer({ orgId, onReportSaved }) {
  const [question, setQuestion] = useState('')
  const [loadingStage, setLoadingStage] = useState(null) // 'generating' | 'running' | null
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [pendingDeep, setPendingDeep] = useState(null)
  const [deepRunning, setDeepRunning] = useState(false)
  // Live partial state during a running deep-analysis job: the same
  // shape as `result` but with some extracted_value cells still null.
  const [deepPartial, setDeepPartial] = useState(null)
  // Tracks whether the user cancelled the current poll loop. We can't
  // cancel the server-side worker from the UI, but we can stop showing
  // updates and free the controls.
  const cancelledRef = useRef(false)

  // If the component unmounts mid-poll, suppress further state updates.
  useEffect(() => () => { cancelledRef.current = true }, [])

  function resetResultStates() {
    setResult(null)
    setError(null)
    setPendingDeep(null)
    setDeepPartial(null)
  }

  async function handleSubmit() {
    const q = question.trim()
    if (!q || loadingStage) return
    resetResultStates()
    setLoadingStage('generating')
    try {
      // Brief micro-delay to give the "Generating query…" label a chance to
      // render before the next stage; mostly visual.
      const respPromise = api.nlQuery(orgId, q)
      setTimeout(() => setLoadingStage(s => s === 'generating' ? 'running' : s), 800)
      const resp = await respPromise
      if (resp.mode === 'needs_deep_analysis') {
        setPendingDeep({
          question: q,
          reason: resp.reason,
          scope_sql: resp.scope_sql,
          estimated_rows: resp.estimated_rows,
        })
      } else {
        setResult({ ...resp, question: q })
      }
    } catch (err) {
      setError(err)
    } finally {
      setLoadingStage(null)
    }
  }

  async function handleConfirmDeep() {
    if (!pendingDeep) return
    const question = pendingDeep.question
    cancelledRef.current = false
    setDeepRunning(true)
    setError(null)
    setDeepPartial(null)
    try {
      const start = await api.startDeepJob(
        orgId,
        question,
        pendingDeep.scope_sql,
      )
      // Seed the partial view from the kickoff response so the user sees
      // a placeholder table while the first batch of extractions runs.
      setDeepPartial({
        question,
        sql: start.sql,
        total_rows: start.total_rows,
        done_rows: 0,
        columns: null,
        rows: null,
      })

      const deadline = Date.now() + DEEP_JOB_POLL_TIMEOUT_MS
      while (!cancelledRef.current) {
        if (Date.now() > deadline) {
          throw new ApiError(
            'Deep analysis took too long. Try again with a tighter scope.',
            { code: 'POLL_TIMEOUT' },
          )
        }
        await new Promise(r => setTimeout(r, DEEP_JOB_POLL_INTERVAL_MS))
        if (cancelledRef.current) return
        const job = await api.getDeepJob(orgId, start.job_id)
        if (cancelledRef.current) return

        if (job.status === 'running') {
          setDeepPartial({
            question,
            sql: job.sql,
            total_rows: job.total_rows,
            done_rows: job.done_rows,
            columns: job.columns,
            rows: job.rows,
          })
          continue
        }
        if (job.status === 'succeeded') {
          setResult({ ...job, question })
          setPendingDeep(null)
          setDeepPartial(null)
          return
        }
        if (job.status === 'failed') {
          throw new ApiError(job.error || 'Deep analysis failed.', {
            code: job.code || 'WORKER_FAILED',
          })
        }
        // Unknown status — bail rather than poll forever.
        throw new ApiError(`Unexpected job status: ${job.status}`, {
          code: 'UNKNOWN_STATUS',
        })
      }
    } catch (err) {
      if (!cancelledRef.current) setError(err)
    } finally {
      if (!cancelledRef.current) {
        setDeepRunning(false)
        setDeepPartial(null)
      }
    }
  }

  function handleCancelDeep() {
    // Stop polling on the client; the server-side worker will finish on
    // its own (and the job row will TTL out in 24h).
    cancelledRef.current = true
    setDeepRunning(false)
    setDeepPartial(null)
    setPendingDeep(null)
  }

  return (
    <div className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Ask a question about your data
        </label>
        <textarea
          value={question}
          onChange={e => setQuestion(e.target.value)}
          rows={3}
          placeholder="e.g. How many failed validations last week by rule?"
          className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          disabled={!!loadingStage || deepRunning}
        />
        <div className="flex items-center gap-3 mt-2">
          <button
            onClick={handleSubmit}
            disabled={!question.trim() || !!loadingStage || deepRunning}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm hover:bg-blue-700 disabled:opacity-50"
          >
            {loadingStage === 'generating' && 'Generating query…'}
            {loadingStage === 'running' && 'Running on Athena…'}
            {!loadingStage && 'Run'}
          </button>
          <div className="flex flex-wrap gap-2">
            {EXAMPLE_QUESTIONS.map(q => (
              <button
                key={q}
                onClick={() => setQuestion(q)}
                disabled={!!loadingStage || deepRunning}
                className="text-xs px-2 py-1 bg-gray-100 text-gray-700 rounded hover:bg-gray-200 disabled:opacity-50"
              >
                {q}
              </button>
            ))}
          </div>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3">
          <p className="text-sm text-red-800">{friendlyError(error)}</p>
          {error.sql && <SqlDetails sql={error.sql} label="Show SQL that was rejected" />}
        </div>
      )}

      {pendingDeep && !result && (
        <DeepConfirmCard
          pending={pendingDeep}
          onConfirm={handleConfirmDeep}
          onCancel={handleCancelDeep}
          running={deepRunning}
        />
      )}

      {deepPartial && !result && (
        <div className="space-y-3">
          <div className="flex items-center gap-3 bg-amber-50 border border-amber-200 rounded p-3">
            <span className="inline-block text-xs px-2 py-0.5 bg-amber-100 text-amber-800 rounded">
              Deep analysis — running
            </span>
            <span className="text-sm text-amber-900">
              Extracted {deepPartial.done_rows} of {deepPartial.total_rows} rows…
            </span>
            <div className="flex-1 h-2 bg-amber-100 rounded overflow-hidden">
              <div
                className="h-full bg-amber-500 transition-all"
                style={{
                  width: deepPartial.total_rows
                    ? `${Math.min(100, (deepPartial.done_rows / deepPartial.total_rows) * 100)}%`
                    : '0%',
                }}
              />
            </div>
            <button
              onClick={handleCancelDeep}
              className="text-sm px-3 py-1 bg-white border border-gray-300 text-gray-700 rounded hover:bg-gray-50"
            >
              Cancel
            </button>
          </div>
          {deepPartial.columns && deepPartial.rows && (
            <>
              <SqlDetails sql={deepPartial.sql} />
              <ResultRenderer
                viz_type="table"
                columns={deepPartial.columns}
                rows={deepPartial.rows}
              />
            </>
          )}
        </div>
      )}

      {result && (
        <div className="space-y-3">
          {result.mode === 'deep' && (
            <span className="inline-block text-xs px-2 py-0.5 bg-amber-100 text-amber-800 rounded">
              Deep analysis — AI-extracted
            </span>
          )}
          {result.explanation && (
            <p className="text-sm text-gray-700">{result.explanation}</p>
          )}
          {result.mode === 'sql' && isApproximate(result.explanation) && (
            <div className="bg-yellow-50 border border-yellow-200 rounded p-2 text-sm text-yellow-800">
              ⚠️ Approximate result — based on free-text keyword matching.
              Phrasing varies across narratives, so counts may be incomplete.
            </div>
          )}
          <SqlDetails sql={result.sql} />
          <ResultRenderer
            viz_type={result.viz_type}
            columns={result.columns}
            rows={result.rows}
          />
          <div className="text-xs text-gray-500">
            {result.row_count} {result.row_count === 1 ? 'row' : 'rows'}
          </div>
          <SaveReportControl
            orgId={orgId}
            result={result}
            onSaved={onReportSaved}
          />
        </div>
      )}
    </div>
  )
}
