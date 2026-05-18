import { useEffect, useRef, useState } from 'react'
import { api, ApiError } from '../api/client.js'
import { ResultRenderer } from './ResultRenderer.jsx'

// Poll cadence for the agent job. 1.5s keeps the step-label UI responsive
// without hammering the API for long runs.
const JOB_POLL_INTERVAL_MS = 1500
// Stop polling after this long even if the server keeps reporting running —
// guards against a runaway worker hanging the UI. Matches worker Lambda
// timeout (10 min) with a bit of headroom.
const JOB_POLL_TIMEOUT_MS = 11 * 60 * 1000

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
  REPORT_TOO_LARGE:
    'This result is too large to save. Reduce the row or column count and re-run.',
  ATHENA_ERROR: 'Athena rejected the generated query. See SQL below.',
  LLM_ERROR: 'The AI model call failed. Try again.',
  WORKER_START_FAILED: 'Could not start the analysis worker. Try again.',
  AGENT_DID_NOT_CONVERGE:
    'The agent ran out of steps without reaching a final answer. Try a tighter question.',
  NO_FINAL_ANSWER:
    'The agent finished without producing a final answer. Try rephrasing the question.',
}

function friendlyError(err) {
  if (err && err.code && ERROR_MESSAGES[err.code]) {
    return ERROR_MESSAGES[err.code]
  }
  return err?.message || 'Something went wrong.'
}

function TraceList({ trace }) {
  if (!trace || trace.length === 0) return null
  return (
    <details className="mt-3 text-sm">
      <summary className="cursor-pointer text-gray-600 hover:text-gray-800">
        Show agent steps ({trace.length})
      </summary>
      <ol className="mt-2 space-y-1 text-xs font-mono bg-gray-50 border border-gray-200 rounded p-3">
        {trace.map((t, i) => (
          <li key={i} className="border-b border-gray-100 last:border-b-0 py-1">
            <span className="text-gray-500">#{t.step}</span>{' '}
            <span className="font-semibold text-gray-800">{t.tool}</span>
            {t.status === 'error' && (
              <span className="ml-2 text-red-700">error: {t.error}</span>
            )}
            {t.assistant_text && (
              <div className="text-purple-700 italic whitespace-pre-wrap break-words">
                thinking: {t.assistant_text}
              </div>
            )}
            {t.input_summary && (
              <div className="text-gray-700 whitespace-pre-wrap break-words">
                in: {t.input_summary}
              </div>
            )}
            {t.output_summary && (
              <div className="text-gray-600 whitespace-pre-wrap break-words">
                out: {t.output_summary}
              </div>
            )}
          </li>
        ))}
      </ol>
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
        sql: '',
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

export function NLAnalyticsExplorer({ orgId, onReportSaved }) {
  const [question, setQuestion] = useState('')
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState(null) // { step_count, current_step_label, trace }
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const cancelledRef = useRef(false)

  useEffect(() => () => { cancelledRef.current = true }, [])

  function reset() {
    setResult(null)
    setError(null)
    setProgress(null)
  }

  async function handleSubmit() {
    const q = question.trim()
    if (!q || running) return
    reset()
    cancelledRef.current = false
    setRunning(true)
    try {
      const start = await api.nlQuery(orgId, q)
      const jobId = start.job_id
      setProgress({ step_count: 0, current_step_label: 'Starting agent…', trace: [] })

      const deadline = Date.now() + JOB_POLL_TIMEOUT_MS
      while (!cancelledRef.current) {
        if (Date.now() > deadline) {
          throw new ApiError(
            'Agent run took too long. Try a tighter question.',
            { code: 'POLL_TIMEOUT' },
          )
        }
        await new Promise(r => setTimeout(r, JOB_POLL_INTERVAL_MS))
        if (cancelledRef.current) return
        const job = await api.getDeepJob(orgId, jobId)
        if (cancelledRef.current) return

        if (job.status === 'running') {
          setProgress({
            step_count: job.step_count || 0,
            current_step_label: job.current_step_label || 'Working…',
            trace: job.trace || [],
          })
          continue
        }
        if (job.status === 'succeeded') {
          setResult({ ...job, question: q })
          setProgress(null)
          return
        }
        if (job.status === 'failed') {
          const err = new ApiError(job.error || 'Agent run failed.', {
            code: job.code || 'WORKER_FAILED',
          })
          err.trace = job.trace || []
          throw err
        }
        throw new ApiError(`Unexpected job status: ${job.status}`, {
          code: 'UNKNOWN_STATUS',
        })
      }
    } catch (err) {
      if (!cancelledRef.current) setError(err)
    } finally {
      if (!cancelledRef.current) {
        setRunning(false)
      }
    }
  }

  function handleCancel() {
    // Stop polling on the client; the server-side worker will finish on
    // its own and the job row will TTL out.
    cancelledRef.current = true
    setRunning(false)
    setProgress(null)
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
          disabled={running}
        />
        <div className="flex items-center gap-3 mt-2">
          <button
            onClick={handleSubmit}
            disabled={!question.trim() || running}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm hover:bg-blue-700 disabled:opacity-50"
          >
            {running ? 'Running…' : 'Run'}
          </button>
          <div className="flex flex-wrap gap-2">
            {EXAMPLE_QUESTIONS.map(q => (
              <button
                key={q}
                onClick={() => setQuestion(q)}
                disabled={running}
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
          {error.sql && (
            <details className="mt-2 text-sm">
              <summary className="cursor-pointer text-gray-600 hover:text-gray-800">
                Show SQL that was rejected
              </summary>
              <pre className="mt-2 bg-gray-50 border border-gray-200 rounded p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap">
                {error.sql}
              </pre>
            </details>
          )}
          <TraceList trace={error.trace} />
        </div>
      )}

      {progress && !result && (
        <div className="bg-amber-50 border border-amber-200 rounded p-3 space-y-2">
          <div className="flex items-center gap-3">
            <span className="inline-block text-xs px-2 py-0.5 bg-amber-100 text-amber-800 rounded">
              Agent — running
            </span>
            <span className="text-sm text-amber-900">
              Step {progress.step_count}: {progress.current_step_label}
            </span>
            <button
              onClick={handleCancel}
              className="ml-auto text-sm px-3 py-1 bg-white border border-gray-300 text-gray-700 rounded hover:bg-gray-50"
            >
              Cancel
            </button>
          </div>
          <TraceList trace={progress.trace} />
        </div>
      )}

      {result && (
        <div className="space-y-3">
          <span className="inline-block text-xs px-2 py-0.5 bg-amber-100 text-amber-800 rounded">
            Agent answer
          </span>
          {result.explanation && (
            <p className="text-sm text-gray-700">{result.explanation}</p>
          )}
          <ResultRenderer
            viz_type={result.viz_type}
            columns={result.columns}
            rows={result.rows}
          />
          <div className="text-xs text-gray-500">
            {result.row_count} {result.row_count === 1 ? 'row' : 'rows'}
          </div>
          <TraceList trace={result.trace} />
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
