import { useEffect, useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { api, ApiError } from '../api/client.js'

export function EligibilityPage() {
  const { orgId } = useParams()
  const [config, setConfig] = useState(null)
  const [configError, setConfigError] = useState(null)
  const [form, setForm] = useState({
    first_name: '',
    last_name: '',
    dob: '',
    ssn: '',
    member_id: '',
    payer_id: '',
    address1: '',
    city: '',
    state: '',
    postal_code: '',
  })
  const [submitting, setSubmitting] = useState(false)
  const [result, setResult] = useState(null)
  const [submitError, setSubmitError] = useState(null)
  const [history, setHistory] = useState([])
  const [copyState, setCopyState] = useState('idle')

  useEffect(() => {
    api.getEligibilityConfig(orgId)
      .then(setConfig)
      .catch((err) => setConfigError(err.message || 'failed to load config'))
  }, [orgId])

  useEffect(() => {
    if (form.first_name && form.last_name && form.dob.length === 8) {
      api.getEligibilityHistory(orgId, {
        first: form.first_name, last: form.last_name, dob: form.dob, limit: 10,
      })
        .then((res) => setHistory(res.history || []))
        .catch(() => setHistory([]))
    } else {
      setHistory([])
    }
  }, [orgId, form.first_name, form.last_name, form.dob])

  const availablePayers = useMemo(() => {
    if (!config?.available_payers) return []
    const preferred = new Set(config.preferred_payer_ids || [])
    const ranked = [...config.available_payers].sort((a, b) => {
      const ap = preferred.has(a.id) ? 0 : 1
      const bp = preferred.has(b.id) ? 0 : 1
      return ap - bp || a.name.localeCompare(b.name)
    })
    return ranked
  }, [config])

  const recentSame = history[0]
  const recentBanner = recentSame
    ? `Already checked by ${recentSame.user_email} at ${formatTimestamp(recentSame.requested_at)} — ${recentSame.payer_name || '?'} ${(recentSame.result_status || '').toUpperCase()}`
    : null

  function update(field, value) {
    setForm((f) => ({ ...f, [field]: value }))
  }

  async function onSubmit(e) {
    e.preventDefault()
    setSubmitting(true)
    setSubmitError(null)
    setResult(null)
    setCopyState('idle')
    try {
      const res = await api.verifyEligibility(orgId, form)
      setResult(res)
    } catch (err) {
      if (err instanceof ApiError && err.code === 'daily_cap_exceeded') {
        setSubmitError('Daily Stedi cap reached for this org. Contact admin to raise the limit.')
      } else {
        setSubmitError(err.message || 'verify failed')
      }
    } finally {
      setSubmitting(false)
    }
  }

  async function copyForCredible() {
    if (!result?.copy_block) return
    try {
      await navigator.clipboard.writeText(result.copy_block)
      setCopyState('copied')
      setTimeout(() => setCopyState('idle'), 2500)
    } catch {
      setCopyState('failed')
    }
  }

  if (configError) {
    return (
      <div className="rounded border border-red-300 bg-red-50 p-4 text-sm text-red-800">
        {configError}
      </div>
    )
  }
  if (!config) {
    return <div className="text-sm text-gray-500">Loading eligibility config…</div>
  }
  if (!config.enabled) {
    return (
      <div className="rounded border border-yellow-300 bg-yellow-50 p-4 text-sm text-yellow-800">
        Stedi eligibility is not enabled for this organization. A super-admin must enable it in config.
      </div>
    )
  }

  return (
    <div>
      <h1 className="text-2xl font-semibold text-gray-900">Insurance Eligibility</h1>
      <p className="mt-1 text-sm text-gray-500">
        Verify active coverage or discover insurance from patient demographics.
        Powered by Stedi /eligibility (270/271) and /insurance-discovery.
      </p>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mt-6">
        <form onSubmit={onSubmit} className="lg:col-span-1 bg-white border border-gray-200 rounded-lg p-4 space-y-3">
          <Field label="First name" required>
            <input type="text" required className={inputCls}
              value={form.first_name}
              onChange={(e) => update('first_name', e.target.value)} />
          </Field>
          <Field label="Last name" required>
            <input type="text" required className={inputCls}
              value={form.last_name}
              onChange={(e) => update('last_name', e.target.value)} />
          </Field>
          <Field label="Date of birth (YYYYMMDD)" required>
            <input type="text" required pattern="\d{8}" className={inputCls}
              value={form.dob}
              onChange={(e) => update('dob', e.target.value.replace(/\D/g, '').slice(0, 8))}
              placeholder="19850315" />
          </Field>
          <Field label="SSN (full or last 4)" hint="Never stored. Required for high-confidence discovery.">
            <input type="password" autoComplete="off" className={inputCls}
              value={form.ssn}
              onChange={(e) => update('ssn', e.target.value)} />
          </Field>
          <Field label="Member ID (optional)" hint="If known, skips discovery and runs eligibility directly.">
            <input type="text" className={inputCls}
              value={form.member_id}
              onChange={(e) => update('member_id', e.target.value)} />
          </Field>
          <Field label="Payer (optional)">
            <select className={inputCls}
              value={form.payer_id}
              onChange={(e) => update('payer_id', e.target.value)}>
              <option value="">— discover —</option>
              {availablePayers.map((p) => (
                <option key={p.id} value={p.id}>{p.name}</option>
              ))}
            </select>
          </Field>

          <details className="text-sm">
            <summary className="cursor-pointer text-gray-600">Address (improves discovery match)</summary>
            <div className="space-y-2 mt-2">
              <input type="text" placeholder="Address 1" className={inputCls}
                value={form.address1}
                onChange={(e) => update('address1', e.target.value)} />
              <div className="grid grid-cols-3 gap-2">
                <input type="text" placeholder="City" className={inputCls}
                  value={form.city}
                  onChange={(e) => update('city', e.target.value)} />
                <input type="text" placeholder="ST" maxLength={2} className={inputCls}
                  value={form.state}
                  onChange={(e) => update('state', e.target.value.toUpperCase())} />
                <input type="text" placeholder="ZIP" className={inputCls}
                  value={form.postal_code}
                  onChange={(e) => update('postal_code', e.target.value)} />
              </div>
            </div>
          </details>

          {recentBanner && (
            <div className="rounded border border-blue-200 bg-blue-50 p-2 text-xs text-blue-800">
              {recentBanner}
            </div>
          )}

          <button type="submit" disabled={submitting}
            className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white font-medium rounded px-4 py-2">
            {submitting ? 'Verifying…' : 'Verify patient'}
          </button>

          {submitError && (
            <div className="rounded border border-red-300 bg-red-50 p-2 text-xs text-red-800">{submitError}</div>
          )}
        </form>

        <div className="lg:col-span-2 space-y-4">
          {result && (
            <ResultCard result={result} onCopy={copyForCredible} copyState={copyState} />
          )}
          <HistoryTable history={history} />
        </div>
      </div>
    </div>
  )
}

// ---- subcomponents ------------------------------------------------------

const inputCls = "w-full rounded border-gray-300 text-sm shadow-sm focus:border-blue-500 focus:ring-blue-500"

function Field({ label, required, hint, children }) {
  return (
    <label className="block">
      <span className="block text-xs font-medium text-gray-700 mb-1">
        {label}{required && <span className="text-red-500"> *</span>}
      </span>
      {children}
      {hint && <span className="block text-xs text-gray-500 mt-1">{hint}</span>}
    </label>
  )
}

function ResultCard({ result, onCopy, copyState }) {
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="text-xs uppercase tracking-wide text-gray-500">Result · path={result.path}</div>
          <h2 className="text-lg font-semibold">
            {result.primary_coverage
              ? `${result.primary_coverage.payer?.name || 'Unknown'} — ${(result.primary_coverage.status || '?').toUpperCase()}`
              : 'No active coverage found'}
          </h2>
        </div>
        <button onClick={onCopy}
          className="bg-emerald-600 hover:bg-emerald-700 text-white text-sm font-medium rounded px-3 py-1.5">
          {copyState === 'copied' ? '✓ Copied' : copyState === 'failed' ? 'Copy failed' : 'Copy for Credible'}
        </button>
      </div>

      {result.discrepancies?.length > 0 && (
        <div className="mb-3 rounded border border-red-300 bg-red-50 p-3 text-sm text-red-800">
          <div className="font-medium mb-1">Discrepancies</div>
          <ul className="list-disc pl-5 space-y-1">
            {result.discrepancies.map((d, i) => <li key={i}>{d}</li>)}
          </ul>
        </div>
      )}

      {result.primary_coverage && <CoverageBlock label="Primary" cov={result.primary_coverage} />}
      {result.secondary_coverages?.map((cov, i) => (
        <CoverageBlock key={i} label={`Secondary #${i + 1}`} cov={cov} />
      ))}

      {result.discovery_review_needed?.length > 0 && (
        <div className="mt-3 rounded border border-yellow-300 bg-yellow-50 p-3 text-sm text-yellow-900">
          <div className="font-medium mb-1">Possible coverage — manual verification required</div>
          <ul className="list-disc pl-5 space-y-1">
            {result.discovery_review_needed.map((item, i) => (
              <li key={i}>
                {item.payer?.name || item.trading_partner_service_id || 'Unknown payer'}
                {item.member_id && <> — Member ID {item.member_id}</>}
                {item.confidence_reason && <span className="text-yellow-700"> ({item.confidence_reason})</span>}
              </li>
            ))}
          </ul>
        </div>
      )}

      <details className="mt-3 text-xs text-gray-500">
        <summary className="cursor-pointer">Copy preview (what gets pasted)</summary>
        <pre className="mt-1 bg-gray-50 border border-gray-200 rounded p-2 whitespace-pre-wrap">{result.copy_block}</pre>
      </details>
    </div>
  )
}

function CoverageBlock({ label, cov }) {
  const sub = cov.subscriber || {}
  const plan = cov.plan || {}
  return (
    <div className="border-t border-gray-100 pt-3 mt-3 text-sm">
      <div className="flex items-center gap-2 mb-2">
        <span className="text-xs font-medium uppercase tracking-wide text-gray-500">{label}</span>
        <StatusPill status={cov.status} />
        {cov.auth_required === true && <span className="text-xs bg-red-100 text-red-700 rounded px-1.5 py-0.5">Auth required</span>}
        {cov.auth_required === false && <span className="text-xs bg-green-100 text-green-700 rounded px-1.5 py-0.5">No auth</span>}
      </div>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-1">
        <Cell k="Payer" v={cov.payer?.name} />
        <Cell k="Plan" v={plan.name} />
        <Cell k="Member ID" v={sub.member_id} />
        <Cell k="Group" v={sub.group_number} />
        <Cell k="Effective" v={plan.effective_date} />
        <Cell k="Expiration" v={plan.expiration_date} />
      </dl>
      {cov.copays?.length > 0 && (
        <div className="mt-2 text-xs text-gray-600">
          Copays: {cov.copays.map((c, i) => <span key={i} className="mr-3">{c.service_type || 'any'}=${c.amount}</span>)}
        </div>
      )}
      {cov.notes?.length > 0 && (
        <div className="mt-2 text-xs text-gray-500 italic">{cov.notes.join(' ')}</div>
      )}
    </div>
  )
}

function Cell({ k, v }) {
  return (
    <>
      <dt className="text-xs text-gray-500">{k}</dt>
      <dd className="text-sm text-gray-900">{v || '—'}</dd>
    </>
  )
}

function StatusPill({ status }) {
  const cls = status === 'active' ? 'bg-green-100 text-green-800'
    : status === 'inactive' ? 'bg-red-100 text-red-800'
    : 'bg-gray-100 text-gray-800'
  return <span className={`text-xs rounded px-2 py-0.5 ${cls}`}>{(status || '?').toUpperCase()}</span>
}

function HistoryTable({ history }) {
  if (!history?.length) return null
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-2">Recent checks for this patient</h3>
      <table className="min-w-full text-sm">
        <thead className="text-xs text-gray-500 uppercase">
          <tr>
            <th className="text-left py-1">When</th>
            <th className="text-left">By</th>
            <th className="text-left">Type</th>
            <th className="text-left">Payer</th>
            <th className="text-left">Status</th>
          </tr>
        </thead>
        <tbody>
          {history.map((row) => (
            <tr key={row.request_id} className="border-t border-gray-100">
              <td className="py-1 text-gray-700">{formatTimestamp(row.requested_at)}</td>
              <td className="text-gray-600">{row.user_email}</td>
              <td className="text-gray-600">{row.call_type}</td>
              <td className="text-gray-600">{row.payer_name || '—'}</td>
              <td><StatusPill status={row.result_status} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function formatTimestamp(iso) {
  if (!iso) return ''
  try {
    const d = new Date(iso)
    return d.toLocaleString()
  } catch {
    return iso
  }
}
