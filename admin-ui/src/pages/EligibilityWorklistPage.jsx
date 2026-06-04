import { useCallback, useEffect, useMemo, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { api } from '../api/client.js'

// Status palette — mirrors the result_status values the FHIR eligibility
// poller writes to DynamoDB. Add new statuses here and in
// fhir_eligibility_poller.py _classify together.
const STATUS_META = {
  verified:             { label: 'Verified',        pill: 'bg-emerald-100 text-emerald-800 border-emerald-200', dot: 'bg-emerald-500',  attention: false },
  discrepancy:          { label: 'Discrepancy',     pill: 'bg-amber-100 text-amber-800 border-amber-200',         dot: 'bg-amber-500',   attention: true  },
  no_coverage:          { label: 'No coverage',     pill: 'bg-red-100 text-red-800 border-red-200',                dot: 'bg-red-500',     attention: true  },
  review_needed:        { label: 'Review needed',   pill: 'bg-yellow-100 text-yellow-800 border-yellow-200',       dot: 'bg-yellow-400',  attention: true  },
  pediatric_no_info:    { label: 'Pediatric — call parent', pill: 'bg-purple-100 text-purple-800 border-purple-200', dot: 'bg-purple-500', attention: true },
  service_type_denied:  { label: 'BH not covered',  pill: 'bg-rose-100 text-rose-800 border-rose-200',             dot: 'bg-rose-500',    attention: true  },
  error:                { label: 'Error',           pill: 'bg-gray-200 text-gray-800 border-gray-300',             dot: 'bg-gray-500',    attention: true  },
}

function patientLabel(item) {
  // Poller-written rows only store initials at the row root (PHI hygiene).
  // Full names live in submitted_demographics for the demo / rerun flow.
  const submitted = item.submitted_demographics || {}
  const first = submitted.first_name || item.patient_first_initial || '?'
  const last = submitted.last_name || item.patient_last_initial || '?'
  return `${first} ${last}`
}

function patientDob(item) {
  return (item.submitted_demographics || {}).dob || ''
}

export function EligibilityWorklistPage() {
  const { orgId } = useParams()
  const [items, setItems] = useState([])
  const [counts, setCounts] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [expandedId, setExpandedId] = useState(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.listEligibilityEncounters(orgId)
      setItems(res.items || [])
      setCounts(res.counts || null)
    } catch (e) {
      setError(e.message || 'failed to load eligibility worklist')
    } finally {
      setLoading(false)
    }
  }, [orgId])

  useEffect(() => { load() }, [load])

  // Fall back to client-side counts if the server didn't send them (e.g.
  // tests with stubbed responses) so the summary pills always render.
  const derivedCounts = useMemo(() => {
    if (counts) return counts
    const c = { total: items.length, verified: 0, discrepancy: 0, no_coverage: 0,
                review_needed: 0, pediatric_no_info: 0, service_type_denied: 0,
                error: 0, attention: 0, resolved: 0 }
    for (const it of items) {
      const s = it.result_status
      if (c[s] !== undefined) c[s] += 1
      const meta = STATUS_META[s]
      const resolved = it.resolution?.state === 'resolved'
      if (resolved) c.resolved += 1
      if (meta?.attention && !resolved) c.attention += 1
    }
    return c
  }, [items, counts])

  async function onResolve(item, note) {
    const updated = await api.resolveEligibilityEncounter(orgId, item.encounter_id, {
      state: 'resolved', note,
    })
    setItems((prev) => prev.map((i) =>
      i.encounter_id === item.encounter_id ? { ...i, resolution: updated.resolution } : i
    ))
    setExpandedId(null)
  }

  async function onRerun(item, correctedDemographics) {
    const res = await api.rerunEligibilityEncounter(orgId, item.encounter_id, correctedDemographics)
    setItems((prev) => prev.map((i) =>
      i.encounter_id === item.encounter_id ? { ...i, ...res.item } : i
    ))
    return res.item
  }

  if (loading) return <div className="text-sm text-gray-500">Loading eligibility worklist…</div>
  if (error) {
    return (
      <div className="rounded border border-red-300 bg-red-50 p-4 text-sm text-red-800">{error}</div>
    )
  }
  if (items.length === 0) {
    return (
      <div>
        <PageHeader orgId={orgId} />
        <div className="mt-6 rounded border border-gray-200 bg-white p-8 text-center text-sm text-gray-500">
          No encounters verified yet. The FHIR poller runs every ~15 minutes for opted-in orgs.
        </div>
      </div>
    )
  }

  return (
    <div>
      <PageHeader orgId={orgId} />

      <SummaryPills counts={derivedCounts} />

      <div className="mt-6 bg-white border border-gray-200 rounded-lg overflow-hidden">
        <table className="min-w-full text-sm">
          <thead className="bg-gray-50 text-xs uppercase text-gray-500">
            <tr>
              <th className="px-3 py-2 text-left w-8"></th>
              <th className="px-3 py-2 text-left">Patient</th>
              <th className="px-3 py-2 text-left">DOB</th>
              <th className="px-3 py-2 text-left">Encounter</th>
              <th className="px-3 py-2 text-left">Status</th>
              <th className="px-3 py-2 text-left">Payer</th>
              <th className="px-3 py-2 text-left">Member ID</th>
              <th className="px-3 py-2 text-left">Notes</th>
              <th className="px-3 py-2 text-left">Resolution</th>
            </tr>
          </thead>
          <tbody>
            {items.map((it) => {
              const expanded = expandedId === it.encounter_id
              return (
                <EncounterRow
                  key={it.encounter_id}
                  item={it}
                  expanded={expanded}
                  onToggle={() => setExpandedId(expanded ? null : it.encounter_id)}
                  onResolve={onResolve}
                  onRerun={onRerun}
                />
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}


function PageHeader({ orgId }) {
  return (
    <div className="flex items-start justify-between">
      <div>
        <h1 className="text-2xl font-semibold text-gray-900">Eligibility Worklist</h1>
        <p className="text-sm text-gray-500 mt-1">
          New encounters from the FHIR feed, verified against Stedi as they arrive.
        </p>
      </div>
      <Link
        to={`/organizations/${orgId}/eligibility`}
        className="text-sm text-blue-600 hover:underline"
      >
        Ad-hoc verify →
      </Link>
    </div>
  )
}


function SummaryPills({ counts }) {
  const pills = [
    { label: 'Total',         count: counts.total,               cls: 'bg-gray-100 text-gray-800' },
    { label: 'Verified',      count: counts.verified,            cls: STATUS_META.verified.pill },
    { label: 'Discrepancy',   count: counts.discrepancy,         cls: STATUS_META.discrepancy.pill },
    { label: 'Review needed', count: counts.review_needed,       cls: STATUS_META.review_needed.pill },
    { label: 'No coverage',   count: counts.no_coverage,         cls: STATUS_META.no_coverage.pill },
    { label: 'Pediatric',     count: counts.pediatric_no_info,   cls: STATUS_META.pediatric_no_info.pill },
    { label: 'BH not covered',count: counts.service_type_denied, cls: STATUS_META.service_type_denied.pill },
  ].filter((p) => p.count > 0 || p.label === 'Total')

  return (
    <div className="mt-4 flex flex-wrap items-center gap-2">
      {pills.map((p) => (
        <span key={p.label} className={`text-xs font-medium rounded px-2.5 py-1 border ${p.cls}`}>
          {p.label}: <strong className="font-semibold">{p.count}</strong>
        </span>
      ))}
      {counts.attention > 0 && (
        <span className="ml-2 text-xs text-gray-600">
          <strong>{counts.attention}</strong> need attention
          {counts.resolved > 0 && <> · {counts.resolved} resolved</>}
        </span>
      )}
      {counts.attention === 0 && counts.resolved > 0 && (
        <span className="ml-2 text-xs text-emerald-700 font-medium">Inbox zero ✓</span>
      )}
    </div>
  )
}


function EncounterRow({ item, expanded, onToggle, onResolve, onRerun }) {
  const meta = STATUS_META[item.result_status] || STATUS_META.error
  const summary = item.result_summary || {}
  const resolved = item.resolution?.state === 'resolved'
  const dimCls = resolved ? 'opacity-60' : ''
  return (
    <>
      <tr className={`border-t border-gray-100 hover:bg-gray-50 cursor-pointer ${dimCls}`} onClick={onToggle}>
        <td className="px-3 py-2"><span className={`inline-block w-2 h-2 rounded-full ${meta.dot}`} /></td>
        <td className="px-3 py-2 text-gray-900 font-medium">{patientLabel(item)}</td>
        <td className="px-3 py-2 text-gray-600">{formatDob(patientDob(item))}</td>
        <td className="px-3 py-2 text-gray-500 font-mono text-xs">
          {(item.encounter_class || '—')}
          <div className="text-gray-400">{formatTime(item.encounter_lastUpdated)}</div>
        </td>
        <td className="px-3 py-2">
          <span className={`text-xs rounded px-2 py-0.5 border ${meta.pill}`}>{meta.label}</span>
          {summary.auth_required === true && (
            <span className="ml-1 text-xs rounded bg-red-100 text-red-800 border border-red-200 px-1.5 py-0.5">Auth required</span>
          )}
          {summary.grace_period_risk === true && (
            <span className="ml-1 text-xs rounded bg-orange-100 text-orange-800 border border-orange-200 px-1.5 py-0.5">Grace risk</span>
          )}
        </td>
        <td className="px-3 py-2 text-gray-700">
          {summary.payer_name || '—'}
          {summary.secondary_count > 0 && <span className="text-gray-400"> +{summary.secondary_count}</span>}
        </td>
        <td className="px-3 py-2 text-gray-600 font-mono text-xs">{summary.member_id_last4 ? `••${summary.member_id_last4}` : '—'}</td>
        <td className="px-3 py-2 text-gray-600 text-xs">
          {(summary.discrepancies || []).slice(0, 1).map((d, i) => <div key={i}>{d}</div>)}
          {(summary.discrepancies || []).length > 1 && (
            <div className="text-gray-400">+{summary.discrepancies.length - 1} more</div>
          )}
          {summary.service_type_status === 'not_covered' && <div className="text-rose-700">Inpatient BH not covered</div>}
        </td>
        <td className="px-3 py-2">
          {resolved ? (
            <span className="text-xs text-emerald-700">✓ Resolved</span>
          ) : (
            <span className="text-xs text-gray-400">—</span>
          )}
        </td>
      </tr>
      {expanded && (
        <tr className="border-t border-gray-100 bg-gray-50">
          <td colSpan={9} className="px-3 py-4">
            <ExpandedDetail item={item} onResolve={onResolve} onRerun={onRerun} />
          </td>
        </tr>
      )}
    </>
  )
}


function ExpandedDetail({ item, onResolve, onRerun }) {
  const summary = item.result_summary || {}
  const [note, setNote] = useState('')
  const [busy, setBusy] = useState(false)
  const resolved = item.resolution?.state === 'resolved'

  async function submit() {
    setBusy(true)
    try {
      await onResolve(item, note)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="space-y-4">
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div>
        <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Result detail</div>
        <dl className="text-sm grid grid-cols-2 gap-x-3 gap-y-1">
          <dt className="text-gray-500">Plan</dt><dd>{summary.plan_name || '—'}</dd>
          <dt className="text-gray-500">Effective</dt><dd>{summary.effective_date || '—'}</dd>
          <dt className="text-gray-500">Expires</dt><dd>{summary.expiration_date || '—'}</dd>
          <dt className="text-gray-500">Active</dt><dd>{String(summary.active ?? '—')}</dd>
          <dt className="text-gray-500">Auth required</dt><dd>{String(summary.auth_required ?? '—')}</dd>
          <dt className="text-gray-500">Secondary plans</dt><dd>{summary.secondary_count || 0}</dd>
        </dl>
        <ServiceTypesTable serviceTypes={summary.service_types} />
        <CobCheckPanel cobCheck={summary.cob_check} />
        {(summary.discrepancies || []).length > 0 && (
          <div className="mt-3 rounded border border-amber-300 bg-amber-50 p-2 text-xs text-amber-900">
            <div className="font-medium mb-1">Discrepancies</div>
            <ul className="list-disc pl-4 space-y-1">
              {summary.discrepancies.map((d, i) => <li key={i}>{d}</li>)}
            </ul>
          </div>
        )}
        {summary.error_kind && (
          <div className="mt-3 rounded border border-red-300 bg-red-50 p-2 text-xs text-red-900">
            <strong>{summary.error_kind}:</strong> {summary.error_message}
          </div>
        )}
      </div>

      <div>
        <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Resolve</div>
        {resolved ? (
          <div className="text-sm">
            <div className="text-emerald-700 font-medium">✓ Resolved</div>
            {item.resolution.note && <div className="text-gray-600 mt-1">{item.resolution.note}</div>}
            <div className="text-xs text-gray-500 mt-2">
              by {item.resolution.resolved_by} at {formatTime(item.resolution.resolved_at)}
            </div>
          </div>
        ) : (
          <div className="space-y-2">
            <textarea
              className="w-full rounded border border-gray-300 text-sm shadow-sm focus:border-blue-500 focus:ring-blue-500"
              rows={2}
              placeholder="Optional note (what you did, who you called, etc.)"
              value={note}
              onChange={(e) => setNote(e.target.value)}
            />
            <button
              type="button"
              disabled={busy}
              onClick={submit}
              className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white text-sm font-medium rounded px-3 py-1.5"
            >
              {busy ? 'Saving…' : 'Mark resolved'}
            </button>
          </div>
        )}
      </div>
    </div>
    <DemographicsPanel item={item} onRerun={onRerun} />
    </div>
  )
}


// Editable demographics + payer-side diff + rerun history.
const DEMOGRAPHIC_LABELS = [
  ['first_name',  'First name'],
  ['middle_name', 'Middle'],
  ['last_name',   'Last name'],
  ['suffix',      'Suffix'],
  ['dob',         'DOB (YYYYMMDD)'],
  ['gender',      'Gender'],
  ['ssn_last4',   'SSN (last 4)'],
  ['address1',    'Address 1'],
  ['address2',    'Address 2'],
  ['city',        'City'],
  ['state',       'State'],
  ['postal_code', 'ZIP'],
]

const PAYER_FIELD_MAP = {
  first_name:  'first_name',
  middle_name: 'middle_name',
  last_name:   'last_name',
  suffix:      'suffix',
  dob:         'dob',
  gender:      'gender',
  address1:    'address1',
  address2:    'address2',
  city:        'city',
  state:       'state',
  postal_code: 'postal_code',
}

function DemographicsPanel({ item, onRerun }) {
  const submitted = item.submitted_demographics || {}
  const corrected = item.corrected_demographics
  const payerDemographics = item.payer_demographics
  const rerunHistory = item.rerun_history || []

  const initial = useMemo(() => ({
    ...submitted, ...(corrected || {}),
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [item.encounter_id, JSON.stringify(corrected)])

  const [edits, setEdits] = useState(initial)
  const [editing, setEditing] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => { setEdits(initial) }, [initial])

  const payerForCompare =
    (payerDemographics?.dependent && Object.keys(payerDemographics.dependent).length > 0)
      ? payerDemographics.dependent
      : payerDemographics?.subscriber

  function diffField(key) {
    const sent = edits[key] || submitted[key] || ''
    const onPayer = payerForCompare?.[PAYER_FIELD_MAP[key]] || ''
    if (!sent || !onPayer) return false
    return String(sent).toUpperCase() !== String(onPayer).toUpperCase()
  }

  function updateField(key, value) {
    setEdits((prev) => ({ ...prev, [key]: value }))
  }

  function reset() {
    setEdits(initial)
    setEditing(false)
    setError(null)
  }

  async function submitRerun() {
    const corrections = {}
    for (const [k] of DEMOGRAPHIC_LABELS) {
      const newVal = (edits[k] || '').trim()
      const oldVal = (submitted[k] || '')
      if (newVal && newVal !== oldVal) corrections[k] = newVal
    }
    if (Object.keys(corrections).length === 0) {
      setError('No changes to submit — edit at least one field first.')
      return
    }
    setBusy(true)
    setError(null)
    try {
      await onRerun(item, corrections)
      setEditing(false)
    } catch (e) {
      setError(e?.message || 'Rerun failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="bg-white border border-gray-200 rounded p-3">
      <div className="flex items-center justify-between mb-3">
        <div className="text-xs font-semibold text-gray-500 uppercase">Patient demographics</div>
        <div className="flex items-center gap-2">
          {!editing ? (
            <button
              type="button"
              onClick={() => setEditing(true)}
              className="text-xs text-blue-600 hover:underline"
            >
              Edit & rerun discovery
            </button>
          ) : (
            <>
              <button
                type="button"
                onClick={reset}
                disabled={busy}
                className="text-xs text-gray-500 hover:text-gray-700"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={submitRerun}
                disabled={busy}
                className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white text-xs font-medium rounded px-2.5 py-1"
              >
                {busy ? 'Rerunning…' : 'Rerun discovery'}
              </button>
            </>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
        <div>
          <div className="text-xs font-medium text-gray-600 mb-1">What we sent</div>
          <table className="w-full text-xs">
            <tbody>
              {DEMOGRAPHIC_LABELS.map(([key, label]) => {
                const diff = diffField(key)
                return (
                  <tr key={key} className="border-t border-gray-100">
                    <td className="py-1 pr-2 text-gray-500 w-1/3">{label}</td>
                    <td className="py-1">
                      {editing ? (
                        <input
                          type="text"
                          className="w-full rounded border-gray-300 text-xs shadow-sm focus:border-blue-500 focus:ring-blue-500"
                          value={edits[key] || ''}
                          onChange={(e) => updateField(key, e.target.value)}
                        />
                      ) : (
                        <span className={diff ? 'text-rose-700 font-medium' : 'text-gray-900'}>
                          {edits[key] || submitted[key] || <span className="text-gray-400">—</span>}
                          {diff && <span className="ml-1 text-xs text-rose-500" title="Differs from payer">≠</span>}
                        </span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        <div>
          <div className="text-xs font-medium text-gray-600 mb-1">
            What the payer has on file
            {payerDemographics?.confidence_level && (
              <span className="ml-2 text-gray-400 font-normal">
                ({payerDemographics.confidence_level})
              </span>
            )}
          </div>
          {payerForCompare ? (
            <table className="w-full text-xs">
              <tbody>
                {DEMOGRAPHIC_LABELS.map(([key, label]) => {
                  const payerVal = payerForCompare[PAYER_FIELD_MAP[key]]
                  const diff = diffField(key)
                  return (
                    <tr key={key} className="border-t border-gray-100">
                      <td className="py-1 pr-2 text-gray-500 w-1/3">{label}</td>
                      <td className={`py-1 ${diff ? 'text-rose-700 font-medium' : 'text-gray-900'}`}>
                        {payerVal || <span className="text-gray-400">—</span>}
                      </td>
                    </tr>
                  )
                })}
                {payerDemographics?.subscriber?.member_id && (
                  <tr className="border-t border-gray-100">
                    <td className="py-1 pr-2 text-gray-500 w-1/3">Member ID</td>
                    <td className="py-1 font-mono">{payerDemographics.subscriber.member_id}</td>
                  </tr>
                )}
                {payerDemographics?.dependent?.relation_to_subscriber && (
                  <tr className="border-t border-gray-100">
                    <td className="py-1 pr-2 text-gray-500 w-1/3">Relation</td>
                    <td className="py-1">{payerDemographics.dependent.relation_to_subscriber} of policyholder</td>
                  </tr>
                )}
              </tbody>
            </table>
          ) : (
            <div className="text-xs text-gray-400 italic">No payer match — nothing to compare against.</div>
          )}
          {payerDemographics?.confidence_reason && (
            <div className="mt-2 text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded p-2">
              {payerDemographics.confidence_reason}
            </div>
          )}
        </div>
      </div>

      {error && (
        <div className="mt-2 text-xs text-rose-700">{error}</div>
      )}

      {rerunHistory.length > 0 && (
        <div className="mt-4 border-t border-gray-100 pt-3">
          <div className="text-xs font-medium text-gray-600 mb-1">Rerun history</div>
          <ul className="text-xs text-gray-700 space-y-1">
            {rerunHistory.map((h, i) => (
              <li key={i} className="flex flex-wrap gap-x-2">
                <span className="text-gray-400">{formatTime(h.rerun_at)}</span>
                <span>by <span className="text-gray-900">{h.rerun_by}</span></span>
                <span className="text-gray-500">
                  · changed {(h.corrected_fields || []).join(', ')}
                </span>
                <span className="text-gray-500">
                  · {h.previous_status} → <span className="text-gray-900">{h.new_status}</span>
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}


const SERVICE_TYPE_STATUS_META = {
  covered:     { label: 'Covered',     cls: 'bg-emerald-100 text-emerald-800 border-emerald-200' },
  not_covered: { label: 'Not covered', cls: 'bg-rose-100 text-rose-800 border-rose-200' },
  unknown:     { label: 'Unknown',     cls: 'bg-gray-100 text-gray-700 border-gray-200' },
}

function ServiceTypesTable({ serviceTypes }) {
  if (!serviceTypes || serviceTypes.length === 0) return null
  return (
    <div className="mt-3">
      <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Service types returned by payer</div>
      <table className="min-w-full text-xs border border-gray-200 rounded">
        <thead className="bg-gray-50 text-gray-500">
          <tr>
            <th className="px-2 py-1 text-left">Code</th>
            <th className="px-2 py-1 text-left">Service</th>
            <th className="px-2 py-1 text-left">Status</th>
            <th className="px-2 py-1 text-left">Auth</th>
            <th className="px-2 py-1 text-left">Copay</th>
          </tr>
        </thead>
        <tbody>
          {serviceTypes.map((st) => {
            const meta = SERVICE_TYPE_STATUS_META[st.status] || SERVICE_TYPE_STATUS_META.unknown
            const copay = (st.copays && st.copays.length > 0)
              ? st.copays.map((c) => `$${c.amount}${c.in_or_out_of_network ? ` (${c.in_or_out_of_network})` : ''}`).join(', ')
              : '—'
            return (
              <tr key={st.code} className="border-t border-gray-100">
                <td className="px-2 py-1 font-mono text-gray-700">{st.code}</td>
                <td className="px-2 py-1 text-gray-900">{st.label}</td>
                <td className="px-2 py-1">
                  <span className={`text-xs rounded px-1.5 py-0.5 border ${meta.cls}`}>{meta.label}</span>
                </td>
                <td className="px-2 py-1 text-gray-700">
                  {st.auth_required === true ? 'Required' : st.auth_required === false ? 'Not required' : '—'}
                </td>
                <td className="px-2 py-1 text-gray-700">{copay}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}


// Coordination-of-Benefits result panel. Only renders when the verify
// flow actually issued a COB call (i.e. ≥2 active coverages AND the org
// has cob_enabled). When status='ok', COB picked a different primary
// than our default "first active wins" rule — that's also flagged in
// the Discrepancies banner above; this panel shows the full ranking.
function CobCheckPanel({ cobCheck }) {
  if (!cobCheck || !cobCheck.checked) return null

  const meta = {
    ok:         { label: 'COB reordered primary',  cls: 'border-amber-300 bg-amber-50 text-amber-900', dot: '⚖' },
    no_change:  { label: 'COB confirmed primacy',  cls: 'border-emerald-300 bg-emerald-50 text-emerald-900', dot: '✓' },
    no_signal:  { label: 'COB ran — no signal',    cls: 'border-gray-300 bg-gray-50 text-gray-700', dot: '•' },
    skipped_cap:{ label: 'COB skipped (daily cap)',cls: 'border-gray-300 bg-gray-50 text-gray-700', dot: '•' },
    error:      { label: 'COB call failed',        cls: 'border-gray-300 bg-gray-50 text-gray-700', dot: '!' },
  }[cobCheck.status] || { label: 'COB ran', cls: 'border-gray-300 bg-gray-50 text-gray-700', dot: '•' }

  const rankings = cobCheck.rankings || []

  return (
    <div className={`mt-3 rounded border p-2 text-xs ${meta.cls}`}>
      <div className="font-medium mb-1">{meta.dot} {meta.label}</div>
      {rankings.length > 0 && (
        <ol className="list-decimal pl-4 space-y-0.5">
          {rankings.map((r, i) => (
            <li key={i}>
              <span className="capitalize">{r.rank}</span>:{' '}
              <span className="font-medium">{r.payer_name || r.payer_id}</span>
              {r.payer_name && r.payer_id && (
                <span className="text-gray-500 font-mono ml-1">({r.payer_id})</span>
              )}
            </li>
          ))}
        </ol>
      )}
      {cobCheck.reason && (
        <div className="mt-1 italic">{cobCheck.reason}</div>
      )}
      {cobCheck.cob_id && (
        <div className="mt-1 text-gray-500 font-mono">trace: {cobCheck.cob_id}</div>
      )}
    </div>
  )
}


function formatTime(iso) {
  if (!iso) return ''
  try { return new Date(iso).toLocaleString() } catch { return iso }
}
function formatDob(yyyymmdd) {
  if (!yyyymmdd || yyyymmdd.length !== 8) return yyyymmdd || ''
  return `${yyyymmdd.slice(4, 6)}/${yyyymmdd.slice(6, 8)}/${yyyymmdd.slice(0, 4)}`
}
