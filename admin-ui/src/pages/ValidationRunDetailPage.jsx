import { useState, useEffect, useMemo, useRef } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { api } from '../api/client.js'
import { OrgWorkspaceLayout } from '../components/OrgWorkspaceLayout.jsx'
import { usePermissions } from '../auth/usePermissions.js'

// Field display labels for different organizations. The keys here are the
// canonical UI field set: only fields whose key is in this map render in
// dynamic-iteration sites (card body grid, evidence panel, filter chips).
// Non-canonical keys still exist on `field_values` — deterministic rules
// and LLM prompts read them — they just don't get shown in the UI, so orgs
// with an unmapped source field see a clean per-org display instead of raw
// vendor plumbing keys leaking through.
const FIELD_LABELS = {
  service_id: 'Service ID',
  date: 'Service Date',
  program: 'Program',
  service_type: 'Service Type',
  diagnosis_code: 'Diagnosis Code',
  bed_day_diagnosis_code: 'Bed Day Diagnosis Code',
  cpt_code: 'CPT Code',
  rate: 'Rate',
  employee_name: 'Employee',
  document_id: 'Document ID',
  payer_description: 'Payer',
}

const CANONICAL_FIELD_KEYS = new Set(Object.keys(FIELD_LABELS))
const isCanonicalField = (key) => CANONICAL_FIELD_KEYS.has(key)

// Fields rendered in the card header (name + link) and therefore skipped
// when iterating over the rest of field_values for the body grid.
const CARD_HEADER_FIELDS = new Set(['employee_name', 'service_id', 'document_id'])

// field_values fields that get dedicated multi-select checkbox filters with
// URL persistence. Anything in this list is excluded from the generic
// single-select per-field chip loop so users see one chip per field, not two.
const MULTI_FILTER_FIELDS = ['program', 'service_type', 'payer_description']

// Fall back to a humanized version of the raw key when an org emits a field
// not in FIELD_LABELS — keeps unknown fields from rendering as e.g.
// "payer_description:" verbatim.
function fieldLabel(key) {
  if (FIELD_LABELS[key]) return FIELD_LABELS[key]
  return key
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
}

// Orgs whose docs come from CentralReach. Grows as more orgs are
// onboarded to the CR pipeline.
const CENTRALREACH_ORGS = new Set(['supportive-care'])

const getCredibleLink = (documentId) =>
  `https://www.cbh3.crediblebh.com/visit/clientvisit_view.asp?clientvisit_id=${documentId}&provportal=0`

const getCentralReachLink = (fileId) =>
  `https://members.centralreach.com/#resources/details?id=${fileId}`

export function getDocumentLink(orgId, doc) {
  if (CENTRALREACH_ORGS.has(orgId)) {
    // preview_file_id is the CR file/resource id from the preview
    // endpoint — the correct input for the resources/details URL. Falls
    // back to document_id for pre-existing records ingested before the
    // field was added; those records will land on the wrong screen but
    // will still open in CR.
    const fv = doc.field_values || {}
    return getCentralReachLink(fv.preview_file_id ?? doc.document_id)
  }
  const fv = doc.field_values || doc
  return getCredibleLink(fv.service_id || doc.document_id)
}

// Rule-status priority for display ordering: FAIL first (most urgent), then
// PASS (confirmed), then SKIP/unknown. Returns a stable sorted copy of the
// rules array without mutating the source.
const STATUS_ORDER = { FAIL: 0, PASS: 1, SKIP: 2 }
function sortRulesByStatus(rules) {
  if (!rules) return []
  return [...rules].sort((a, b) => {
    const av = STATUS_ORDER[a?.status] ?? 3
    const bv = STATUS_ORDER[b?.status] ?? 3
    return av - bv
  })
}

// TODO(brian): the whole "required fields" gate is tech debt — we're
// hiding documents from the UI based on hardcoded field names
// (diagnosis_code + employee_name) and then poking hardcoded service-type
// escape hatches (BedDay-Psych, BedDay-Detox) when those fields are
// legitimately absent. Every new service type that doesn't carry those
// fields means another silent "why isn't this showing up?" bug and another
// string added to this set.
//
// Replace this with a server-driven signal — e.g. the API tells us which
// docs are renderable, or each doc declares its own required-field schema
// per service type — so this file doesn't need to know which service
// types are special. Until then, this list will drift.
const BEDDAY_SERVICE_TYPES = new Set(['BedDay-Psych', 'BedDay-Detox'])
// Orgs whose docs bypass the diagnosis_code/employee_name gate entirely.
// supportive-care's centralreach-ingested docs don't carry those fields
// (they're Credible-BH-shaped columns) so the gate would hide every one.
// See the TODO above — this is another string in a growing list until the
// gate is replaced with a server-side renderability signal.
const UNGATED_ORGS = new Set(['supportive-care'])
export function hasRequiredFields(doc, orgId) {
  if (UNGATED_ORGS.has(orgId)) return true
  const fv = doc?.field_values
  if (!fv) return false
  if (BEDDAY_SERVICE_TYPES.has(fv.service_type)) return true
  return Boolean(fv.diagnosis_code && fv.employee_name)
}

// Run IDs are emitted as YYYYMMDD-HHMMSS (e.g. "20260421-153039"), so we
// can recover the run execution time even when the detail API doesn't echo
// a `timestamp` field. Returns null if the id doesn't match the expected shape.
function parseRunIdTimestamp(runId) {
  if (!runId) return null
  const m = /^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})$/.exec(runId)
  if (!m) return null
  const [, y, mo, d, h, mi, s] = m
  const t = new Date(+y, +mo - 1, +d, +h, +mi, +s).getTime()
  return Number.isNaN(t) ? null : t
}

export function ValidationRunDetailPage() {
  const { orgId, runId } = useParams()
  const { canViewAnalytics } = usePermissions()
  const canViewRevenue = canViewAnalytics('revenue_analysis')
  const [searchParams, setSearchParams] = useSearchParams()
  const docIdFromUrl = searchParams.get('doc')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [selectedDoc, setSelectedDoc] = useState(null)
  const [selectedRule, setSelectedRule] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [ruleFilter, setRuleFilter] = useState('all')
  // If deep-linking to a doc, start with 'all' filter so the doc is visible
  const [statusFilter, setStatusFilter] = useState(docIdFromUrl ? 'all' : 'needs_action')
  const [confirmingRuleId, setConfirmingRuleId] = useState(null)
  const [resolvingRuleId, setResolvingRuleId] = useState(null)
  const [markingIncorrectRuleId, setMarkingIncorrectRuleId] = useState(null)
  const [incorrectFeedbackText, setIncorrectFeedbackText] = useState('')
  const [incorrectOutcome, setIncorrectOutcome] = useState('PASS')
  const [submittingIncorrect, setSubmittingIncorrect] = useState(false)
  const [confirmingDocument, setConfirmingDocument] = useState(false)
  const [categoryFilter, setCategoryFilter] = useState('all')
  // Multi-select filters for specific field_values fields. Each entry is a Set
  // of allowed values (OR semantics); an empty set means "no filter". Hydrated
  // from the URL on mount, and written back to the URL on change so links are
  // shareable. URL keys use the full field name (e.g. ?payer_description=A,B).
  const [multiFilters, setMultiFilters] = useState(() => {
    const init = {}
    for (const name of MULTI_FILTER_FIELDS) {
      const raw = searchParams.get(name)
      init[name] = new Set(raw ? raw.split(',').filter(Boolean) : [])
    }
    return init
  })
  // "Validation report date" — filters by the run.timestamp. Because every
  // doc on this page belongs to the same run, this ends up all-or-nothing.
  const [dateFilter, setDateFilter] = useState('all')
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')
  // "Service date" — filters per-doc by field_values.date.
  const [serviceDateFilter, setServiceDateFilter] = useState('all')
  const [serviceCustomStartDate, setServiceCustomStartDate] = useState('')
  const [serviceCustomEndDate, setServiceCustomEndDate] = useState('')
  // Per-field_values filter map: { fieldName: selectedValue }. A field is
  // considered active when its entry exists and isn't 'all'. We use a single
  // map rather than per-field useState because the field list is org-driven
  // and only known after data loads.
  const [fieldFilters, setFieldFilters] = useState({})
  // The detail endpoint doesn't echo a run timestamp, so grab it from the
  // runs-list endpoint (same source the Validation Results tab uses).
  const [runTimestamp, setRunTimestamp] = useState(null)

  // Update one multi-select filter and sync to the URL. Empty set removes the
  // query param entirely so a "no filters" state has a clean URL. Other query
  // params (e.g. ?doc=...) are preserved.
  const updateMultiFilter = (field, nextSet) => {
    setMultiFilters(prev => ({ ...prev, [field]: nextSet }))
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (nextSet.size === 0) next.delete(field)
      else next.set(field, Array.from(nextSet).join(','))
      return next
    }, { replace: true })
  }

  // Load validation run data - only depends on orgId and runId
  useEffect(() => {
    setLoading(true)
    api.getValidationRun(orgId, runId)
      .then(result => {
        setData(result)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [orgId, runId])

  // Handle document selection after data loads
  // This runs when data changes OR when docIdFromUrl changes
  useEffect(() => {
    if (!data?.documents) return

    // Priority 1: Select document from URL query param if present
    if (docIdFromUrl) {
      const targetDoc = data.documents.find(d => String(d.document_id) === String(docIdFromUrl))
      if (targetDoc) {
        setSelectedDoc(targetDoc)
        const firstFailedRule = targetDoc.rules?.find(r => r.status === 'FAIL')
        setSelectedRule(firstFailedRule || targetDoc.rules?.[0] || null)
        return
      }
    }

    // Priority 2: Auto-select first document with failures (only if no doc is selected yet)
    if (!selectedDoc) {
      const firstFailed = data.documents.find(d => d.summary?.failed > 0 && hasRequiredFields(d, orgId))
      if (firstFailed) {
        setSelectedDoc(firstFailed)
        const firstFailedRule = firstFailed.rules?.find(r => r.status === 'FAIL')
        if (firstFailedRule) setSelectedRule(firstFailedRule)
      } else {
        const firstValidDoc = data.documents.find(d => hasRequiredFields(d, orgId))
        if (firstValidDoc) {
          setSelectedDoc(firstValidDoc)
          if (firstValidDoc.rules?.length > 0) {
            setSelectedRule(firstValidDoc.rules[0])
          }
        }
      }
    }
  }, [data, docIdFromUrl]) // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch the run's own timestamp from the list endpoint — the detail payload
  // doesn't include one. Failures are silent; the filter falls back to the
  // parsed run-ID timestamp (which the list rows use identical).
  useEffect(() => {
    let cancelled = false
    api.listValidationRuns(orgId)
      .then(resp => {
        if (cancelled) return
        const list = (Array.isArray(resp) ? resp : resp?.runs) || []
        const match = list.find(r => r.validation_run_id === runId)
        if (match?.timestamp) setRunTimestamp(match.timestamp)
      })
      .catch(() => { /* fall back to run-id parsing */ })
    return () => { cancelled = true }
  }, [orgId, runId])

  // Helper function to check if all failed rules in a document have been confirmed (but not yet fixed)
  const allFailedRulesConfirmed = (doc) => {
    const failedRules = doc.rules?.filter(r => r.status === 'FAIL') || []
    if (failedRules.length === 0) return false // No failures means not applicable
    // All must be confirmed AND at least one not yet fixed
    const allConfirmedOrFixed = failedRules.every(r => r.finding_confirmed || r.fixed)
    const anyNotFixed = failedRules.some(r => !r.fixed)
    return allConfirmedOrFixed && anyNotFixed
  }

  // Helper function to check if all failed rules in a document have been fixed
  const allFailedRulesFixed = (doc) => {
    const failedRules = doc.rules?.filter(r => r.status === 'FAIL') || []
    if (failedRules.length === 0) return false // No failures means not applicable
    return failedRules.every(r => r.fixed)
  }

  // Compute summary stats. "Passed" and "Confirmed" are both no-FAIL states;
  // Confirmed is Passed + an explicit reviewer sign-off via `document_confirmed`.
  // See docs/validation-workflow-states.md for the full state machine.
  const stats = useMemo(() => {
    if (!data?.documents) return { needsAction: 0, awaitingStaff: 0, passed: 0, confirmed: 0, revenueAtRisk: 0 }

    let needsAction = 0
    let awaitingStaff = 0
    let passed = 0
    let confirmed = 0
    let revenueAtRisk = 0

    // Drop duplicate documents sharing a service_id (keep first occurrence)
    // so they don't double-count in the summary cards.
    const seenServiceIds = new Set()

    data.documents.forEach(doc => {
      // BedDay service types are exempt from the diagnosis_code/employee_name gate
      if (!hasRequiredFields(doc, orgId)) {
        return
      }

      const sid = doc.field_values?.service_id
      if (sid != null && sid !== '') {
        if (seenServiceIds.has(sid)) return
        seenServiceIds.add(sid)
      }

      const failedRules = doc.rules?.filter(r => r.status === 'FAIL') || []
      if (failedRules.length > 0) {
        if (allFailedRulesFixed(doc)) {
          // All failed rules fixed — passed (reviewer may still doc-confirm).
          if (doc.document_confirmed) confirmed++
          else passed++
        } else if (allFailedRulesConfirmed(doc)) {
          awaitingStaff++
        } else {
          needsAction++
          const rate = parseFloat(doc.field_values?.rate) || 0
          revenueAtRisk += rate
        }
      } else if (doc.document_confirmed) {
        confirmed++
      } else {
        passed++
      }
    })

    return { needsAction, awaitingStaff, passed, confirmed, revenueAtRisk }
  }, [data])

  // Get unique categories and rules for filters
  const { categories, rules } = useMemo(() => {
    if (!data?.documents) return { categories: [], rules: [] }

    const categorySet = new Set()
    const ruleSet = new Set()

    data.documents.forEach(doc => {
      doc.rules?.forEach(rule => {
        if (rule.category) categorySet.add(rule.category)
        const name = rule.rule_name || rule.rule_id
        if (name) ruleSet.add(name)
      })
    })

    return {
      categories: Array.from(categorySet).sort(),
      rules: Array.from(ruleSet).sort(),
    }
  }, [data])

  // Distinct values for each multi-select field, drawn from this run's docs.
  // Keys absent from any doc end up with an empty array → the chip auto-hides.
  const multiFilterOptions = useMemo(() => {
    const out = {}
    for (const field of MULTI_FILTER_FIELDS) out[field] = new Set()
    if (!data?.documents) return Object.fromEntries(MULTI_FILTER_FIELDS.map(f => [f, []]))
    data.documents.forEach(doc => {
      const fv = doc.field_values
      if (!fv) return
      for (const field of MULTI_FILTER_FIELDS) {
        const v = fv[field]
        if (v != null && v !== '') out[field].add(String(v))
      }
    })
    return Object.fromEntries(
      MULTI_FILTER_FIELDS.map(f => [f, Array.from(out[f]).sort()])
    )
  }, [data])

  // Per-field_values dropdowns are generated from whatever field names show up
  // in the run's documents — different orgs emit different schemas. Skip the
  // fields that already have dedicated filters (the MULTI_FILTER_FIELDS multi-
  // selects, plus service date) and document_id (high-cardinality, covered by
  // free-text search).
  const FIELD_FILTER_EXCLUDE = useMemo(
    () => new Set([...MULTI_FILTER_FIELDS, 'date', 'document_id']),
    []
  )
  const fieldValueOptions = useMemo(() => {
    if (!data?.documents) return []
    const byField = new Map()
    data.documents.forEach(doc => {
      const fv = doc.field_values
      if (!fv) return
      Object.entries(fv).forEach(([name, value]) => {
        // Canonical UI fields only. Non-canonical keys never get a filter
        // chip — matches the card body / evidence panel policy.
        if (!isCanonicalField(name)) return
        if (FIELD_FILTER_EXCLUDE.has(name)) return
        if (value == null || value === '') return
        const str = String(value)
        if (!byField.has(name)) byField.set(name, new Set())
        byField.get(name).add(str)
      })
    })
    return Array.from(byField.entries())
      .map(([name, values]) => ({ name, values: Array.from(values).sort() }))
      .sort((a, b) => a.name.localeCompare(b.name))
  }, [data, FIELD_FILTER_EXCLUDE])

  // Filter documents
  const filteredDocs = useMemo(() => {
    if (!data?.documents) return []

    // Two independent date dimensions:
    //   - Validation report date → filters on run.timestamp (all-or-nothing for
    //     this single-run view).
    //   - Service date → filters per-doc by field_values.date.
    const dayMs = 24 * 60 * 60 * 1000
    const now = Date.now()
    const parseLocal = (str) => {
      if (!str) return null
      const [y, m, d] = str.split('-').map(Number)
      if (!y || !m || !d) return null
      return new Date(y, m - 1, d).getTime()
    }

    let startCutoff = null
    let endCutoff = null
    if (dateFilter === '24h') startCutoff = now - dayMs
    else if (dateFilter === '7d') startCutoff = now - 7 * dayMs
    else if (dateFilter === '30d') startCutoff = now - 30 * dayMs
    else if (dateFilter === '90d') startCutoff = now - 90 * dayMs
    else if (dateFilter === 'custom') {
      if (customStartDate) startCutoff = parseLocal(customStartDate)
      if (customEndDate) { const e = parseLocal(customEndDate); if (e != null) endCutoff = e + dayMs }
    }

    let svcStart = null
    let svcEnd = null
    if (serviceDateFilter === '24h') svcStart = now - dayMs
    else if (serviceDateFilter === '7d') svcStart = now - 7 * dayMs
    else if (serviceDateFilter === '30d') svcStart = now - 30 * dayMs
    else if (serviceDateFilter === '90d') svcStart = now - 90 * dayMs
    else if (serviceDateFilter === 'custom') {
      if (serviceCustomStartDate) svcStart = parseLocal(serviceCustomStartDate)
      if (serviceCustomEndDate) { const e = parseLocal(serviceCustomEndDate); if (e != null) svcEnd = e + dayMs }
    }
    const serviceFilterActive = svcStart != null || svcEnd != null
    // Prefer the timestamp we fetched from the list endpoint (same source
    // that renders the Date column on the Validation Results tab). Fall back
    // to any timestamp on the detail payload, then to parsing the run id.
    let runTimestampMs = null
    const candidates = [runTimestamp, data?.timestamp]
    for (const c of candidates) {
      if (!c) continue
      const parsed = new Date(c).getTime()
      if (!Number.isNaN(parsed)) { runTimestampMs = parsed; break }
    }
    if (runTimestampMs == null) runTimestampMs = parseRunIdTimestamp(runId)

    const dateFilterActive = startCutoff != null || endCutoff != null
    // Fail closed when a window is set but we can't determine the run time —
    // better to show nothing than to silently ignore the user's filter.
    const runPassesDateFilter = !dateFilterActive
      ? true
      : runTimestampMs == null
        ? false
        : (
            (startCutoff == null || runTimestampMs >= startCutoff) &&
            (endCutoff == null || runTimestampMs < endCutoff)
          )

    // Short-circuit: if the run itself falls outside the date window, no
    // docs from this run are shown.
    if (!runPassesDateFilter) return []

    // Drop duplicate documents that share a service_id, keeping the first
    // occurrence. Docs without a service_id are always kept.
    const seenServiceIds = new Set()
    const dedupedDocs = data.documents.filter(doc => {
      const sid = doc.field_values?.service_id
      if (sid == null || sid === '') return true
      if (seenServiceIds.has(sid)) return false
      seenServiceIds.add(sid)
      return true
    })

    return dedupedDocs.filter(doc => {
      // BedDay service types are exempt from the diagnosis_code/employee_name gate
      if (!hasRequiredFields(doc, orgId)) {
        return false
      }

      // Service date filter (per-doc)
      if (serviceFilterActive) {
        const raw = doc.field_values?.date
        if (!raw) return false
        const t = new Date(raw).getTime()
        if (Number.isNaN(t)) return false
        if (svcStart != null && t < svcStart) return false
        if (svcEnd != null && t >= svcEnd) return false
      }

      // Search filter
      if (searchTerm) {
        const search = searchTerm.toLowerCase()
        const matchesId = doc.document_id?.toLowerCase().includes(search)
        const matchesEmployee = doc.field_values?.employee_name?.toLowerCase().includes(search)
        const matchesProgram = doc.field_values?.program?.toLowerCase().includes(search)
        if (!matchesId && !matchesEmployee && !matchesProgram) return false
      }

      // Status filter (from the Needs Action / Awaiting Staff / Passed / Confirmed
      // summary cards at top). Passed and Confirmed are both no-FAIL states;
      // Confirmed = Passed + explicit reviewer sign-off.
      const failedRules = doc.rules?.filter(r => r.status === 'FAIL') || []
      const hasFailures = failedRules.length > 0
      const noOpenFailures = !hasFailures || allFailedRulesFixed(doc)
      if (statusFilter === 'needs_action') {
        if (!(hasFailures && !allFailedRulesConfirmed(doc) && !allFailedRulesFixed(doc))) return false
      }
      if (statusFilter === 'awaiting_staff') {
        if (!(hasFailures && allFailedRulesConfirmed(doc) && !allFailedRulesFixed(doc))) return false
      }
      if (statusFilter === 'passed') {
        if (!(noOpenFailures && !doc.document_confirmed)) return false
      }
      if (statusFilter === 'confirmed') {
        if (!(noOpenFailures && doc.document_confirmed)) return false
      }

      // Rule filter: the rule result we look for on each doc depends on the
      // active status filter:
      //   Needs Action        → rule must have FAILED
      //   Passed / Confirmed  → rule must NOT have FAILED (PASS or SKIP both
      //                         count, matching the no-open-FAIL bucket
      //                         definition). This way every rule in the
      //                         dropdown is reachable — rules that never pass
      //                         but only skip still surface their docs here.
      //   All statuses        → rule just has to exist on the doc (any status)
      if (ruleFilter !== 'all') {
        const hasMatchingRule = doc.rules?.some(r => {
          if ((r.rule_name || r.rule_id) !== ruleFilter) return false
          if (statusFilter === 'needs_action') return r.status === 'FAIL'
          if (statusFilter === 'passed' || statusFilter === 'confirmed') return r.status !== 'FAIL'
          return true
        })
        if (!hasMatchingRule) return false
      }

      // Multi-select filters (program, service_type, payer_description). OR
      // semantics within a field, AND across fields. Empty set = no filter.
      for (const field of MULTI_FILTER_FIELDS) {
        const set = multiFilters[field]
        if (!set || set.size === 0) continue
        const raw = doc.field_values?.[field]
        if (raw == null || !set.has(String(raw))) return false
      }

      // Dynamic per-field_values filters (single-select for everything else).
      for (const [name, selected] of Object.entries(fieldFilters)) {
        if (!selected || selected === 'all') continue
        const raw = doc.field_values?.[name]
        if (raw == null || String(raw) !== selected) return false
      }

      // Category filter (document has at least one rule in category)
      if (categoryFilter !== 'all') {
        const hasCategory = doc.rules?.some(r => r.category === categoryFilter)
        if (!hasCategory) return false
      }

      // Date filter is applied at the run level above, not per doc.

      return true
    })
  }, [data, runId, runTimestamp, searchTerm, statusFilter, ruleFilter, multiFilters, categoryFilter, dateFilter, customStartDate, customEndDate, serviceDateFilter, serviceCustomStartDate, serviceCustomEndDate, fieldFilters])

  // Track if this is the first render to skip the statusFilter effect on mount
  const isFirstRender = useRef(true)

  // When status filter changes (user clicks a filter), select the first document in the filtered list
  useEffect(() => {
    // Skip on first render - let the document selection effect handle initial selection
    if (isFirstRender.current) {
      isFirstRender.current = false
      return
    }

    if (filteredDocs.length > 0) {
      const firstDoc = filteredDocs[0]
      setSelectedDoc(firstDoc)
      // Select the first failed rule if exists, otherwise first rule
      const firstFailedRule = firstDoc.rules?.find(r => r.status === 'FAIL')
      setSelectedRule(firstFailedRule || firstDoc.rules?.[0] || null)
    } else {
      setSelectedDoc(null)
      setSelectedRule(null)
    }
  }, [statusFilter, filteredDocs])

  if (loading) return <OrgWorkspaceLayout><div className="flex items-center justify-center h-64"><p className="text-gray-500">Loading validation run...</p></div></OrgWorkspaceLayout>
  if (error) return <OrgWorkspaceLayout><div className="p-4"><p className="text-red-600">Error: {error}</p></div></OrgWorkspaceLayout>
  if (!data) return <OrgWorkspaceLayout><div className="p-4"><p className="text-gray-500">Validation run not found</p></div></OrgWorkspaceLayout>

  return (
    <OrgWorkspaceLayout>
    <div className="h-full flex flex-col">
      {/* Summary Cards */}
      <div className={`grid ${canViewRevenue ? 'grid-cols-5' : 'grid-cols-4'} gap-4 mb-6`}>
        <SummaryCard
          label="NEEDS ACTION"
          value={stats.needsAction}
          color="red"
          active={statusFilter === 'needs_action'}
          onClick={() => setStatusFilter(statusFilter === 'needs_action' ? 'all' : 'needs_action')}
        />
        <SummaryCard
          label="AWAITING STAFF"
          value={stats.awaitingStaff}
          color="yellow"
          active={statusFilter === 'awaiting_staff'}
          onClick={() => setStatusFilter(statusFilter === 'awaiting_staff' ? 'all' : 'awaiting_staff')}
        />
        <SummaryCard
          label="PASSED"
          value={stats.passed}
          color="green"
          active={statusFilter === 'passed'}
          onClick={() => setStatusFilter(statusFilter === 'passed' ? 'all' : 'passed')}
        />
        <SummaryCard
          label="CONFIRMED"
          value={stats.confirmed}
          color="green"
          active={statusFilter === 'confirmed'}
          onClick={() => setStatusFilter(statusFilter === 'confirmed' ? 'all' : 'confirmed')}
        />
        {canViewRevenue && (
          <SummaryCard
            label="REVENUE AT RISK"
            value={`$${stats.revenueAtRisk.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
            subtext={`${stats.needsAction + stats.awaitingStaff} blocked claim${(stats.needsAction + stats.awaitingStaff) !== 1 ? 's' : ''}`}
            color="blue"
            onClick={() => {}}
          />
        )}
      </div>

      {/* Search and Filters — compact pill style matching the other dashboard pages */}
      <div className="flex items-center gap-2 mb-4 flex-wrap">
        {/* Search pill */}
        <div className="flex-1 min-w-[260px] relative">
          <input
            type="text"
            placeholder="Search by ID, employee, or program..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-9 pr-8 py-1.5 bg-white border border-gray-200 rounded-full text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <svg className="absolute left-3 top-2 h-4 w-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          {searchTerm && (
            <button
              onClick={() => setSearchTerm('')}
              className="absolute right-2 top-1.5 text-gray-400 hover:text-gray-600 p-0.5"
              aria-label="Clear search"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          )}
        </div>

        <FilterChip
          active={ruleFilter !== 'all'}
          iconPath="M7 7h.01M7 3h5c.512 0 1.024.195 1.414.586l7 7a2 2 0 010 2.828l-7 7a2 2 0 01-2.828 0l-7-7A1.994 1.994 0 013 12V7a4 4 0 014-4z"
          value={ruleFilter}
          onChange={setRuleFilter}
          maxSelectWidth="max-w-[180px]"
        >
          <option value="all">All rules</option>
          {rules.map(r => <option key={r} value={r}>{r}</option>)}
        </FilterChip>

        {MULTI_FILTER_FIELDS.map(field => {
          const options = multiFilterOptions[field] || []
          if (options.length === 0) return null
          return (
            <MultiSelectFilterChip
              key={`multi-${field}`}
              label={FIELD_LABELS[field] || fieldLabel(field)}
              iconPath="M5 13l4 4L19 7"
              options={options}
              selected={multiFilters[field]}
              onChange={(next) => updateMultiFilter(field, next)}
            />
          )
        })}

        <FilterChip
          active={categoryFilter !== 'all'}
          iconPath="M7 7h.01M7 3h5c.512 0 1.024.195 1.414.586l7 7a2 2 0 010 2.828l-7 7a2 2 0 01-2.828 0l-7-7A1.994 1.994 0 013 12V7a4 4 0 014-4z"
          value={categoryFilter}
          onChange={setCategoryFilter}
        >
          <option value="all">All categories</option>
          {categories.map(c => <option key={c} value={c}>{c}</option>)}
        </FilterChip>

        {fieldValueOptions.map(({ name, values }) => {
          const label = FIELD_LABELS[name] || name
          const selected = fieldFilters[name] || 'all'
          return (
            <FilterChip
              key={`fv-${name}`}
              active={selected !== 'all'}
              iconPath="M4 6h16M4 12h8m-8 6h16"
              value={selected}
              onChange={(v) => setFieldFilters(prev => ({ ...prev, [name]: v }))}
              label={label}
            >
              <option value="all">All</option>
              {values.map(v => <option key={v} value={v}>{v}</option>)}
            </FilterChip>
          )
        })}

        <FilterChip
          active={dateFilter !== 'all'}
          iconPath="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"
          value={dateFilter}
          onChange={setDateFilter}
          label="Report"
        >
          <option value="all">All dates</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
          <option value="90d">Last 90 days</option>
          <option value="custom">Custom range</option>
        </FilterChip>

        <FilterChip
          active={serviceDateFilter !== 'all'}
          iconPath="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"
          value={serviceDateFilter}
          onChange={setServiceDateFilter}
          label="Service"
        >
          <option value="all">All service dates</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
          <option value="90d">Last 90 days</option>
          <option value="custom">Custom range</option>
        </FilterChip>
      </div>

      {(dateFilter === 'custom' || serviceDateFilter === 'custom') && (
        <div className="flex flex-col gap-2 mb-4">
          {dateFilter === 'custom' && (
            <CustomDateRange
              label="Report date range"
              start={customStartDate}
              onStartChange={setCustomStartDate}
              end={customEndDate}
              onEndChange={setCustomEndDate}
              onClear={() => { setDateFilter('all'); setCustomStartDate(''); setCustomEndDate('') }}
            />
          )}
          {serviceDateFilter === 'custom' && (
            <CustomDateRange
              label="Service date range"
              start={serviceCustomStartDate}
              onStartChange={setServiceCustomStartDate}
              end={serviceCustomEndDate}
              onEndChange={setServiceCustomEndDate}
              onClear={() => { setServiceDateFilter('all'); setServiceCustomStartDate(''); setServiceCustomEndDate('') }}
            />
          )}
        </div>
      )}

      {/* Split Panel */}
      <div className="flex-1 flex gap-4 min-h-0">
        {/* Left Panel - Document List */}
        <div className="w-1/3 bg-white rounded-lg shadow overflow-hidden flex flex-col">
          <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between">
            <span className="text-sm font-medium text-gray-700">
              {filteredDocs.length} Document{filteredDocs.length !== 1 ? 's' : ''}
            </span>
          </div>
          <div className="flex-1 overflow-y-auto max-h-[calc(5*9rem)]">
            {filteredDocs.map(doc => (
              <DocumentListItem
                key={doc.document_id}
                doc={doc}
                orgId={orgId}
                selected={selectedDoc?.document_id === doc.document_id}
                onClick={() => {
                  setSelectedDoc(doc)
                  // Rule filter is active → surface the matching rule on the
                  // doc (same status context rules as the list filter). If no
                  // rule filter, fall back to the first failing rule.
                  const ruleMatch = ruleFilter !== 'all'
                    ? doc.rules?.find(r => {
                        if ((r.rule_name || r.rule_id) !== ruleFilter) return false
                        if (statusFilter === 'needs_action') return r.status === 'FAIL'
                        if (statusFilter === 'passed' || statusFilter === 'confirmed') return r.status !== 'FAIL'
                        return true
                      })
                    : null
                  const firstFailedRule = doc.rules?.find(r => r.status === 'FAIL')
                  setSelectedRule(ruleMatch || firstFailedRule || doc.rules?.[0] || null)
                }}
              />
            ))}
            {filteredDocs.length === 0 && (
              <p className="p-4 text-sm text-gray-500">No documents match your filters.</p>
            )}
          </div>
        </div>

        {/* Right Panel - Detail View */}
        <div className="flex-1 bg-white rounded-lg shadow overflow-hidden flex flex-col">
          {selectedDoc ? (
            <DocumentDetailPanel
              doc={selectedDoc}
              orgId={orgId}
              selectedRule={selectedRule}
              onSelectRule={setSelectedRule}
              confirmingRuleId={confirmingRuleId}
              onConfirmFinding={async (ruleId) => {
                setConfirmingRuleId(ruleId)
                try {
                  await api.confirmFinding(orgId, runId, selectedDoc.document_id, ruleId)
                  // Update the local state to reflect the rule confirmation
                  const timestamp = new Date().toISOString()
                  setData(prev => ({
                    ...prev,
                    documents: prev.documents.map(d =>
                      d.document_id === selectedDoc.document_id
                        ? {
                            ...d,
                            rules: d.rules.map(r =>
                              r.rule_id === ruleId
                                ? { ...r, finding_confirmed: true, finding_confirmed_at: timestamp }
                                : r
                            )
                          }
                        : d
                    )
                  }))
                  // Update selected doc's rules
                  setSelectedDoc(prev => ({
                    ...prev,
                    rules: prev.rules.map(r =>
                      r.rule_id === ruleId
                        ? { ...r, finding_confirmed: true, finding_confirmed_at: timestamp }
                        : r
                    )
                  }))
                  // Update selected rule if it's the one we just confirmed
                  if (selectedRule?.rule_id === ruleId) {
                    setSelectedRule(prev => ({ ...prev, finding_confirmed: true, finding_confirmed_at: timestamp }))
                  }
                } catch (err) {
                  setError(`Failed to confirm finding: ${err.message}`)
                } finally {
                  setConfirmingRuleId(null)
                }
              }}
              resolvingRuleId={resolvingRuleId}
              onMarkResolved={async (ruleId) => {
                setResolvingRuleId(ruleId)
                try {
                  await api.markResolved(orgId, runId, selectedDoc.document_id, ruleId)
                  // Update local state: set fixed=true, remove finding_confirmed
                  const timestamp = new Date().toISOString()
                  setData(prev => ({
                    ...prev,
                    documents: prev.documents.map(d =>
                      d.document_id === selectedDoc.document_id
                        ? {
                            ...d,
                            rules: d.rules.map(r =>
                              r.rule_id === ruleId
                                ? { ...r, fixed: true, fixed_at: timestamp, finding_confirmed: undefined, finding_confirmed_at: undefined }
                                : r
                            )
                          }
                        : d
                    )
                  }))
                  // Update selected doc's rules
                  setSelectedDoc(prev => ({
                    ...prev,
                    rules: prev.rules.map(r =>
                      r.rule_id === ruleId
                        ? { ...r, fixed: true, fixed_at: timestamp, finding_confirmed: undefined, finding_confirmed_at: undefined }
                        : r
                    )
                  }))
                  // Update selected rule if it's the one we just resolved
                  if (selectedRule?.rule_id === ruleId) {
                    setSelectedRule(prev => ({ ...prev, fixed: true, fixed_at: timestamp, finding_confirmed: undefined, finding_confirmed_at: undefined }))
                  }
                } catch (err) {
                  setError(`Failed to mark resolved: ${err.message}`)
                } finally {
                  setResolvingRuleId(null)
                }
              }}
              markingIncorrectRuleId={markingIncorrectRuleId}
              setMarkingIncorrectRuleId={setMarkingIncorrectRuleId}
              incorrectFeedbackText={incorrectFeedbackText}
              setIncorrectFeedbackText={setIncorrectFeedbackText}
              submittingIncorrect={submittingIncorrect}
              incorrectOutcome={incorrectOutcome}
              setIncorrectOutcome={setIncorrectOutcome}
              onMarkIncorrect={async (ruleId, feedbackText, outcome) => {
                setSubmittingIncorrect(true)
                try {
                  const rule = selectedDoc.rules.find(r => r.rule_id === ruleId)
                  await api.enhanceNote(
                    orgId,
                    feedbackText,
                    rule?.rule_text || '',
                    selectedDoc.document_id,
                    runId,
                    ruleId,
                    rule?.notes || []
                  )
                  await api.markIncorrect(orgId, runId, selectedDoc.document_id, ruleId, outcome)

                  const timestamp = new Date().toISOString()
                  const patch = { status: outcome, feedback_given: true, feedback_given_at: timestamp }
                  setData(prev => ({
                    ...prev,
                    documents: prev.documents.map(d =>
                      d.document_id === selectedDoc.document_id
                        ? { ...d, rules: d.rules.map(r => r.rule_id === ruleId ? { ...r, ...patch } : r) }
                        : d
                    )
                  }))
                  setSelectedDoc(prev => ({
                    ...prev,
                    rules: prev.rules.map(r => r.rule_id === ruleId ? { ...r, ...patch } : r)
                  }))
                  if (selectedRule?.rule_id === ruleId) {
                    setSelectedRule(prev => ({ ...prev, ...patch }))
                  }

                  setMarkingIncorrectRuleId(null)
                  setIncorrectFeedbackText('')
                  setIncorrectOutcome('PASS')
                } catch (err) {
                  setError(`Failed to submit feedback: ${err.message}`)
                } finally {
                  setSubmittingIncorrect(false)
                }
              }}
              confirmingDocument={confirmingDocument}
              onConfirmDocument={async () => {
                setConfirmingDocument(true)
                try {
                  const resp = await api.confirmDocument(orgId, runId, selectedDoc.document_id)
                  const patch = {
                    document_confirmed: true,
                    document_confirmed_at: resp.document_confirmed_at,
                    document_confirmed_by: resp.document_confirmed_by,
                  }
                  setData(prev => ({
                    ...prev,
                    documents: prev.documents.map(d =>
                      d.document_id === selectedDoc.document_id ? { ...d, ...patch } : d
                    )
                  }))
                  setSelectedDoc(prev => ({ ...prev, ...patch }))
                } catch (err) {
                  setError(`Failed to confirm document: ${err.message}`)
                } finally {
                  setConfirmingDocument(false)
                }
              }}
            />
          ) : (
            <div className="flex-1 flex items-center justify-center text-gray-500">
              Select a document to view details
            </div>
          )}
        </div>
      </div>
    </div>
    </OrgWorkspaceLayout>
  )
}


// Pill-style filter dropdown: icon + borderless inline select inside a
// rounded chip. When a non-default value is picked the chip tints blue
// so it's clear something's filtered at a glance.
function FilterChip({ active, iconPath, value, onChange, children, maxSelectWidth = 'max-w-[160px]', label }) {
  return (
    <div
      className={`inline-flex items-center gap-1.5 rounded-full pl-3 pr-1 py-0.5 border shadow-sm transition-colors ${
        active
          ? 'bg-blue-50 border-blue-200'
          : 'bg-white border-gray-200 hover:border-gray-300'
      }`}
    >
      <svg
        className={`w-3.5 h-3.5 ${active ? 'text-blue-500' : 'text-gray-400'}`}
        fill="none" stroke="currentColor" viewBox="0 0 24 24"
      >
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={iconPath} />
      </svg>
      {label && (
        <span className={`text-[10px] font-semibold uppercase tracking-wider ${active ? 'text-blue-700' : 'text-gray-500'}`}>
          {label}
        </span>
      )}
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={`text-xs font-medium bg-transparent border-0 focus:outline-none focus:ring-0 pr-1 py-1 cursor-pointer truncate ${maxSelectWidth} ${
          active ? 'text-blue-700' : 'text-gray-700'
        }`}
      >
        {children}
      </select>
    </div>
  )
}

// Multi-select chip: pill that opens a popover with checkbox list. Used for
// field_values fields where users want OR semantics ("Aetna or Medicare").
// `selected` is a Set; `onChange` receives a new Set. Search box auto-shows
// when option count exceeds 30 (keeps long payer lists scannable without
// adding ceremony for short ones).
function MultiSelectFilterChip({ label, iconPath, options, selected, onChange }) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const rootRef = useRef(null)
  const searchable = options.length > 30

  useEffect(() => {
    if (!open) return
    const onDown = (e) => {
      if (rootRef.current && !rootRef.current.contains(e.target)) setOpen(false)
    }
    const onKey = (e) => { if (e.key === 'Escape') setOpen(false) }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const active = selected.size > 0
  const summary = selected.size === 0
    ? `All ${label.toLowerCase()}`
    : selected.size === 1
      ? Array.from(selected)[0]
      : `${selected.size} selected`

  const filtered = query
    ? options.filter(o => o.toLowerCase().includes(query.toLowerCase()))
    : options

  const toggle = (value) => {
    const next = new Set(selected)
    if (next.has(value)) next.delete(value)
    else next.add(value)
    onChange(next)
  }

  return (
    <div ref={rootRef} className="relative inline-block">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className={`inline-flex items-center gap-1.5 rounded-full pl-3 pr-3 py-1 border shadow-sm transition-colors ${
          active
            ? 'bg-blue-50 border-blue-200 text-blue-700'
            : 'bg-white border-gray-200 hover:border-gray-300 text-gray-700'
        }`}
      >
        <svg
          className={`w-3.5 h-3.5 ${active ? 'text-blue-500' : 'text-gray-400'}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={iconPath} />
        </svg>
        <span className="text-[10px] font-semibold uppercase tracking-wider">
          {label}
        </span>
        <span className="text-xs font-medium truncate max-w-[160px]">
          {summary}
        </span>
        <svg
          className={`w-3 h-3 transition-transform ${open ? 'rotate-180' : ''} ${active ? 'text-blue-500' : 'text-gray-400'}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div className="absolute z-20 mt-1 left-0 w-64 bg-white border border-gray-200 rounded-lg shadow-lg">
          <div className="flex items-center justify-between px-3 py-2 border-b border-gray-100">
            <button
              type="button"
              onClick={() => onChange(new Set(options))}
              className="text-xs text-blue-600 hover:text-blue-800"
            >
              Select all
            </button>
            <button
              type="button"
              onClick={() => onChange(new Set())}
              className="text-xs text-gray-500 hover:text-gray-700"
            >
              Clear
            </button>
          </div>
          {searchable && (
            <div className="px-2 py-2 border-b border-gray-100">
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search..."
                className="w-full px-2 py-1 border border-gray-200 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
          )}
          <div className="max-h-64 overflow-y-auto py-1">
            {filtered.length === 0 ? (
              <div className="px-3 py-2 text-xs text-gray-400">No matches</div>
            ) : filtered.map(opt => (
              <label
                key={opt}
                className="flex items-center gap-2 px-3 py-1.5 hover:bg-gray-50 cursor-pointer"
              >
                <input
                  type="checkbox"
                  checked={selected.has(opt)}
                  onChange={() => toggle(opt)}
                  className="rounded text-blue-600 focus:ring-blue-500"
                />
                <span className="text-xs text-gray-700 truncate">{opt}</span>
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function CustomDateRange({ label, start, onStartChange, end, onEndChange, onClear }) {
  return (
    <div className="flex items-end gap-3 flex-wrap">
      <span className="text-xs font-medium text-gray-500 uppercase tracking-wide self-center">
        {label}
      </span>
      <div className="flex flex-col">
        <label className="text-[10px] font-medium text-gray-500 uppercase tracking-wide mb-0.5">From</label>
        <input
          type="date"
          value={start}
          onChange={(e) => onStartChange(e.target.value)}
          className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>
      <div className="flex flex-col">
        <label className="text-[10px] font-medium text-gray-500 uppercase tracking-wide mb-0.5">To</label>
        <input
          type="date"
          value={end}
          onChange={(e) => onEndChange(e.target.value)}
          className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>
      <button
        onClick={onClear}
        className="text-sm text-blue-600 hover:text-blue-800 px-2 py-2"
      >
        Clear
      </button>
    </div>
  )
}


function SummaryCard({ label, value, subtext, color, active, onClick }) {
  // Active = soft transparent tint, matching the card's accent color.
  const activeStyles = {
    red: 'border-red-300 bg-red-500/10',
    yellow: 'border-yellow-300 bg-yellow-500/10',
    green: 'border-green-300 bg-green-500/10',
    blue: 'border-blue-300 bg-blue-500/10',
  }

  const textStyles = {
    red: 'text-red-700',
    yellow: 'text-yellow-700',
    green: 'text-green-700',
    blue: 'text-blue-700',
  }

  return (
    <button
      onClick={onClick}
      className={`p-4 rounded-lg border-2 text-left transition-all ${
        active ? activeStyles[color] : 'border-gray-200 bg-white hover:border-gray-300'
      }`}
    >
      <div className={`text-2xl font-bold ${active ? textStyles[color] : 'text-gray-900'}`}>
        {value}
      </div>
      <div className={`text-xs font-medium uppercase tracking-wide ${active ? textStyles[color] : 'text-gray-500'}`}>
        {label}
      </div>
      {subtext && (
        <div className="text-xs text-gray-400 mt-1">{subtext}</div>
      )}
    </button>
  )
}


function DocumentListItem({ doc, orgId, selected, onClick }) {
  const failedRules = doc.rules?.filter(r => r.status === 'FAIL') || []
  const failCount = failedRules.length
  const hasFailures = failCount > 0
  const fixedCount = failedRules.filter(r => r.fixed).length
  const confirmedOrFixedCount = failedRules.filter(r => r.finding_confirmed || r.fixed).length
  const allFixed = hasFailures && fixedCount === failCount
  const allConfirmedOrFixed = hasFailures && confirmedOrFixedCount === failCount && !allFixed
  const fv = doc.field_values || {}

  return (
    <div
      onClick={onClick}
      className={`px-4 py-3 border-b border-gray-100 cursor-pointer transition-colors ${
        selected ? 'bg-blue-50 border-l-4 border-l-blue-500' : 'hover:bg-gray-50'
      }`}
    >
      {/* Header row: Employee name + status badges */}
      <div className="flex items-start justify-between mb-1">
        {fv.employee_name ? (
          <span className="text-sm font-medium text-gray-900">
            {fv.employee_name}
          </span>
        ) : (
          <a
            href={getDocumentLink(orgId, doc)}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm font-medium text-blue-600 hover:text-blue-800 hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            {fv.service_id || doc.document_id}
          </a>
        )}
        <div className="flex items-center gap-1">
          {allFixed && (
            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-800">
              Resolved
            </span>
          )}
          {allConfirmedOrFixed && (
            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-800">
              Awaiting Staff
            </span>
          )}
          {hasFailures && !allConfirmedOrFixed && !allFixed && (
            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800">
              {confirmedOrFixedCount > 0 ? `${failCount - confirmedOrFixedCount}/${failCount}` : failCount} fail{failCount !== 1 ? 's' : ''}
            </span>
          )}
        </div>
      </div>

      {/* Field values — render canonical UI fields only (per FIELD_LABELS).
          employee_name, service_id, document_id are rendered in the header
          above and skipped here to avoid duplication. Non-canonical keys
          exist on `fv` for rule input but are intentionally hidden so the
          card stays consistent across orgs. */}
      <div className="text-xs text-gray-500 space-y-1">
        {Object.entries(fv)
          .filter(([k, v]) =>
            v != null && v !== ''
            && !CARD_HEADER_FIELDS.has(k)
            && isCanonicalField(k)
          )
          .map(([k, v]) => (
            <div key={k} className="flex items-center gap-1">
              <span className="text-gray-400">{fieldLabel(k)}:</span>
              <span className="text-gray-600">{k === 'rate' ? `$${v}` : String(v)}</span>
            </div>
          ))}
      </div>

      {/* Rule status indicators — sorted FAIL → PASS → SKIP */}
      <div className="flex gap-1 mt-2">
        {sortRulesByStatus(doc.rules).slice(0, 8).map((rule, idx) => (
          <div
            key={idx}
            className={`w-2 h-2 rounded-full ${
              rule.status === 'PASS' ? 'bg-green-400' :
              rule.status === 'FAIL' ? 'bg-red-400' :
              'bg-gray-300'
            }`}
            title={`${rule.rule_name}: ${rule.status}`}
          />
        ))}
        {doc.rules?.length > 8 && (
          <span className="text-xs text-gray-400">+{doc.rules.length - 8}</span>
        )}
      </div>
    </div>
  )
}


function DocumentDetailPanel({ doc, orgId, selectedRule, onSelectRule, confirmingRuleId, onConfirmFinding, resolvingRuleId, onMarkResolved, markingIncorrectRuleId, setMarkingIncorrectRuleId, incorrectFeedbackText, setIncorrectFeedbackText, incorrectOutcome, setIncorrectOutcome, submittingIncorrect, onMarkIncorrect, confirmingDocument, onConfirmDocument }) {
  const failedRules = doc.rules?.filter(r => r.status === 'FAIL') || []
  const passedRules = doc.rules?.filter(r => r.status === 'PASS') || []
  const skippedRules = doc.rules?.filter(r => r.status === 'SKIP') || []
  const fixedCount = failedRules.filter(r => r.fixed).length
  const confirmedOrFixedCount = failedRules.filter(r => r.finding_confirmed || r.fixed).length
  const allFixed = failedRules.length > 0 && fixedCount === failedRules.length
  const allConfirmedOrFixed = failedRules.length > 0 && confirmedOrFixedCount === failedRules.length
  const isDocConfirmed = !!doc.document_confirmed
  const canConfirmDoc = !isDocConfirmed && (doc.rules || []).every(r => r.status === 'PASS' || r.status === 'SKIP')

  return (
    <div className="flex flex-col h-full">
      {/* Document Header */}
      <div className="px-4 py-3 border-b border-gray-200 bg-gray-50">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-medium text-gray-900">
              {doc.field_values?.employee_name || 'Document'}
            </h3>
            <p className="text-sm text-gray-500">
              ID:{' '}
              <a
                href={getDocumentLink(orgId, doc)}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:text-blue-800 hover:underline"
              >
                {doc.field_values?.service_id || doc.document_id}
              </a>
            </p>
          </div>
          <div className="text-right text-sm">
            <div className="text-gray-500">{doc.field_values?.program}</div>
            <div className="text-gray-400">{doc.field_values?.date}</div>
          </div>
        </div>
        {/* Show confirmation/resolution progress or completed status */}
        {(failedRules.length > 0 || isDocConfirmed || canConfirmDoc) && (
          <div className="mt-3 flex items-center justify-between gap-2 flex-wrap">
            <div className="flex items-center gap-2">
              {isDocConfirmed && (
                <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-green-100 text-green-800">
                  DOCUMENT CONFIRMED
                  {doc.document_confirmed_by && (
                    <span className="ml-1 font-normal text-green-700">
                      by {doc.document_confirmed_by}
                    </span>
                  )}
                </span>
              )}
              {!isDocConfirmed && allFixed && (
                <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-green-100 text-green-800">
                  All Findings Resolved
                </span>
              )}
              {!isDocConfirmed && !allFixed && allConfirmedOrFixed && (
                <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-yellow-100 text-yellow-800">
                  {fixedCount > 0 ? `${fixedCount}/${failedRules.length} Resolved - ` : ''}Awaiting Staff Action
                </span>
              )}
              {!isDocConfirmed && !allFixed && !allConfirmedOrFixed && confirmedOrFixedCount > 0 && (
                <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-orange-100 text-orange-800">
                  {confirmedOrFixedCount}/{failedRules.length} Confirmed/Resolved
                </span>
              )}
            </div>
            {canConfirmDoc && (
              <button
                onClick={onConfirmDocument}
                disabled={confirmingDocument}
                className="px-3 py-1.5 bg-green-600 text-white text-xs font-medium rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {confirmingDocument ? 'Confirming…' : 'Confirm Document'}
              </button>
            )}
          </div>
        )}
      </div>

      {/* Rules Tabs */}
      <div className="px-4 py-2 border-b border-gray-200 flex gap-4">
        <RuleTab
          label="Failed"
          count={failedRules.length}
          color="red"
          rules={failedRules}
          selectedRule={selectedRule}
          onSelectRule={onSelectRule}
        />
        <RuleTab
          label="Skipped"
          count={skippedRules.length}
          color="gray"
          rules={skippedRules}
          selectedRule={selectedRule}
          onSelectRule={onSelectRule}
        />
        <RuleTab
          label="Passed"
          count={passedRules.length}
          color="green"
          rules={passedRules}
          selectedRule={selectedRule}
          onSelectRule={onSelectRule}
        />
      </div>

      {/* Rule Selector — sorted FAIL → PASS → SKIP */}
      <div className="px-4 py-2 border-b border-gray-200 bg-gray-50 overflow-x-auto">
        <div className="flex gap-2">
          {sortRulesByStatus(doc.rules).map((rule, idx) => (
            <button
              key={idx}
              onClick={() => onSelectRule(rule)}
              className={`px-3 py-1.5 rounded-full text-xs font-medium whitespace-nowrap transition-colors ${
                selectedRule === rule
                  ? rule.status === 'FAIL' ? 'bg-red-600 text-white' :
                    rule.status === 'PASS' ? 'bg-green-600 text-white' :
                    'bg-gray-500 text-white'
                  : rule.status === 'FAIL' ? 'bg-red-100 text-red-700 hover:bg-red-200' :
                    rule.status === 'PASS' ? 'bg-green-100 text-green-700 hover:bg-green-200' :
                    'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              {rule.rule_name || rule.rule_id}
            </button>
          ))}
        </div>
      </div>

      {/* Selected Rule Detail */}
      <div className="flex-1 overflow-y-auto p-4">
        {selectedRule ? (
          <RuleDetailView
            rule={selectedRule}
            fieldValues={doc.field_values}
            confirmingRuleId={confirmingRuleId}
            onConfirmFinding={onConfirmFinding}
            resolvingRuleId={resolvingRuleId}
            onMarkResolved={onMarkResolved}
            markingIncorrectRuleId={markingIncorrectRuleId}
            setMarkingIncorrectRuleId={setMarkingIncorrectRuleId}
            incorrectFeedbackText={incorrectFeedbackText}
            setIncorrectFeedbackText={setIncorrectFeedbackText}
            incorrectOutcome={incorrectOutcome}
            setIncorrectOutcome={setIncorrectOutcome}
            submittingIncorrect={submittingIncorrect}
            onMarkIncorrect={onMarkIncorrect}
            isDocConfirmed={isDocConfirmed}
          />
        ) : (
          <p className="text-gray-500">Select a rule to view details</p>
        )}
      </div>
    </div>
  )
}


function RuleTab({ label, count, color }) {
  const colorStyles = {
    red: 'text-red-600',
    gray: 'text-gray-600',
    green: 'text-green-600',
  }

  return (
    <div className="flex items-center gap-1.5">
      <span className={`font-medium ${colorStyles[color]}`}>{count}</span>
      <span className="text-sm text-gray-500">{label}</span>
    </div>
  )
}


function RuleDetailView({ rule, fieldValues, confirmingRuleId, onConfirmFinding, resolvingRuleId, onMarkResolved, markingIncorrectRuleId, setMarkingIncorrectRuleId, incorrectFeedbackText, setIncorrectFeedbackText, incorrectOutcome, setIncorrectOutcome, submittingIncorrect, onMarkIncorrect, isDocConfirmed }) {
  // Extract reasoning from message (format: "STATUS - reasoning")
  const extractReasoning = () => {
    const message = rule.message || ''
    const status = rule.status || ''
    if (message.startsWith(`${status} - `)) {
      return message.substring(status.length + 3)
    }
    if (message.startsWith(`${status}: `)) {
      return message.substring(status.length + 2)
    }
    return message || 'No reasoning provided.'
  }

  const statusColors = {
    FAIL: { bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-200' },
    PASS: { bg: 'bg-green-100', text: 'text-green-800', border: 'border-green-200' },
    SKIP: { bg: 'bg-gray-100', text: 'text-gray-700', border: 'border-gray-200' },
    ERROR: { bg: 'bg-gray-100', text: 'text-gray-800', border: 'border-gray-200' },
  }

  const colors = statusColors[rule.status] || statusColors.ERROR
  const isConfirming = confirmingRuleId === rule.rule_id
  const isResolving = resolvingRuleId === rule.rule_id
  const isMarkingIncorrect = markingIncorrectRuleId === rule.rule_id
  const isFailed = rule.status === 'FAIL'
  const isSkipped = rule.status === 'SKIP'
  const isConfirmed = rule.finding_confirmed
  const isFixed = rule.fixed
  const hasFeedbackGiven = rule.feedback_given
  const isLlmRule = rule.rule_type !== 'deterministic'  // Default is 'llm' when not set
  // Doc-confirmed locks all rule mutations. See docs/validation-workflow-states.md.
  const canMutate = !isDocConfirmed

  return (
    <div className="space-y-4">
      {/* Tags */}
      <div className="flex flex-wrap gap-2">
        <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium ${colors.bg} ${colors.text}`}>
          {rule.status}
        </span>
        {rule.category && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-blue-100 text-blue-800">
            {rule.category}
          </span>
        )}
        {isFailed && !isConfirmed && !isFixed && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-orange-100 text-orange-800">
            BILLING BLOCKER
          </span>
        )}
        {isFailed && isConfirmed && !isFixed && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-yellow-100 text-yellow-800">
            FINDING CONFIRMED
          </span>
        )}
        {isFailed && isFixed && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-green-100 text-green-800">
            RESOLVED
          </span>
        )}
        {hasFeedbackGiven && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-purple-100 text-purple-800">
            FEEDBACK GIVEN
          </span>
        )}
      </div>

      {/* Rule Name */}
      <div>
        <h4 className="text-lg font-semibold text-gray-900">{rule.rule_name || rule.rule_id}</h4>
        {rule.rule_id && rule.rule_name && (
          <p className="text-sm text-gray-500">Rule ID: {rule.rule_id}</p>
        )}
      </div>

      {/* Reasoning */}
      <div className={`p-4 rounded-lg border ${colors.border} ${colors.bg}`}>
        <h5 className="text-sm font-medium text-gray-700 mb-2">Reasoning</h5>
        <p className="text-sm text-gray-800">{extractReasoning()}</p>
      </div>

      {/* Recommended Next Steps */}
      {isFailed && !isConfirmed && !isFixed && (
        <div className="p-4 rounded-lg border border-blue-200 bg-blue-50">
          <h5 className="text-sm font-medium text-blue-800 mb-2">Recommended Next Step</h5>
          <p className="text-sm text-blue-700">
            Review the documentation for this service to verify compliance with billing requirements.
            Update the chart if corrections are needed before resubmitting for validation.
          </p>
        </div>
      )}

      {/* Confirm Finding and Mark Incorrect Buttons - only for failed rules that haven't been confirmed or fixed */}
      {canMutate && isFailed && !isConfirmed && !isFixed && !isMarkingIncorrect && (
        <div className="pt-2">
          <div className="flex gap-2">
            <button
              onClick={() => onConfirmFinding(rule.rule_id)}
              disabled={isConfirming}
              className="px-4 py-2 bg-yellow-500 text-white text-sm font-medium rounded-md hover:bg-yellow-600 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isConfirming ? 'Confirming...' : 'Confirm Finding'}
            </button>
            {isLlmRule && (
              <button
                onClick={() => { setIncorrectOutcome('PASS'); setMarkingIncorrectRuleId(rule.rule_id); }}
                className="px-4 py-2 bg-gray-500 text-white text-sm font-medium rounded-md hover:bg-gray-600"
              >
                Mark Incorrect
              </button>
            )}
          </div>
          <p className="text-xs text-gray-500 mt-2">
            {isLlmRule
              ? 'Confirm if the finding is correct and needs staff action, or mark as incorrect if this is a false positive.'
              : 'Confirm if the finding is correct and needs staff action.'}
          </p>
        </div>
      )}

      {/* Mark Incorrect entry for SKIPPED rules — reviewer picks the true outcome */}
      {canMutate && isSkipped && !hasFeedbackGiven && !isMarkingIncorrect && isLlmRule && (
        <div className="pt-2">
          <button
            onClick={() => { setIncorrectOutcome('PASS'); setMarkingIncorrectRuleId(rule.rule_id); }}
            className="px-4 py-2 bg-gray-500 text-white text-sm font-medium rounded-md hover:bg-gray-600"
          >
            Mark Incorrect
          </button>
          <p className="text-xs text-gray-500 mt-2">
            This rule was skipped. If it should have evaluated, mark it incorrect and choose whether it should have passed or failed.
          </p>
        </div>
      )}

      {/* Mark Incorrect Feedback Form (used for both FAIL false-positive and SKIP recategorization) */}
      {isMarkingIncorrect && (
        <div className="p-4 rounded-lg border border-gray-300 bg-gray-50">
          {isSkipped && (
            <div className="mb-3">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                What should the true outcome have been?
              </label>
              <div className="flex gap-2">
                <label className="inline-flex items-center gap-1.5 text-sm text-gray-700">
                  <input
                    type="radio"
                    name="incorrect-outcome"
                    value="PASS"
                    checked={incorrectOutcome === 'PASS'}
                    onChange={() => setIncorrectOutcome('PASS')}
                    disabled={submittingIncorrect}
                  />
                  Passed
                </label>
                <label className="inline-flex items-center gap-1.5 text-sm text-gray-700 ml-4">
                  <input
                    type="radio"
                    name="incorrect-outcome"
                    value="FAIL"
                    checked={incorrectOutcome === 'FAIL'}
                    onChange={() => setIncorrectOutcome('FAIL')}
                    disabled={submittingIncorrect}
                  />
                  Failed
                </label>
              </div>
            </div>
          )}
          <label className="block text-sm font-medium text-gray-700 mb-2">
            {isSkipped ? 'Why should this rule not have been skipped?' : 'Why is this finding incorrect?'}
          </label>
          <textarea
            value={incorrectFeedbackText}
            onChange={(e) => setIncorrectFeedbackText(e.target.value)}
            placeholder={isSkipped
              ? 'Explain why this rule should have evaluated for this document...'
              : 'Explain why this rule validation is incorrect...'}
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            rows={3}
          />
          <div className="mt-3 flex gap-2">
            <button
              onClick={() => onMarkIncorrect(rule.rule_id, incorrectFeedbackText, isSkipped ? incorrectOutcome : 'PASS')}
              disabled={submittingIncorrect || !incorrectFeedbackText.trim()}
              className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {submittingIncorrect ? 'Submitting...' : 'Submit Feedback'}
            </button>
            <button
              onClick={() => { setMarkingIncorrectRuleId(null); setIncorrectFeedbackText(''); setIncorrectOutcome('PASS'); }}
              disabled={submittingIncorrect}
              className="px-4 py-2 bg-gray-200 text-gray-700 text-sm font-medium rounded-md hover:bg-gray-300 disabled:opacity-50"
            >
              Cancel
            </button>
          </div>
          <p className="text-xs text-gray-500 mt-2">
            {isSkipped
              ? `Your feedback will be used to improve rule accuracy. The rule will be recorded as ${isSkipped ? (incorrectOutcome === 'FAIL' ? 'FAILED' : 'PASSED') : 'passed'}.`
              : 'Your feedback will be used to improve rule accuracy. The finding will be marked as passed.'}
          </p>
        </div>
      )}

      {/* Confirmed status with Mark Resolved button */}
      {isFailed && isConfirmed && !isFixed && (
        <div className="p-4 rounded-lg border border-yellow-200 bg-yellow-50">
          <h5 className="text-sm font-medium text-yellow-800 mb-1">Finding Confirmed</h5>
          <p className="text-sm text-yellow-700">
            This finding has been reviewed and is awaiting staff action.
            {rule.finding_confirmed_at && (
              <span className="block text-xs text-yellow-600 mt-1">
                Confirmed at: {new Date(rule.finding_confirmed_at).toLocaleString()}
              </span>
            )}
          </p>
          {canMutate && (
            <div className="mt-3">
              <button
                onClick={() => onMarkResolved(rule.rule_id)}
                disabled={isResolving}
                className="px-4 py-2 bg-green-600 text-white text-sm font-medium rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isResolving ? 'Marking Resolved...' : 'Mark Resolved'}
              </button>
              <p className="text-xs text-yellow-600 mt-2">
                Mark this finding as resolved after staff has addressed it in the source system.
              </p>
            </div>
          )}
        </div>
      )}

      {/* Resolved status with timestamp */}
      {isFailed && isFixed && (
        <div className="p-4 rounded-lg border border-green-200 bg-green-50">
          <h5 className="text-sm font-medium text-green-800 mb-1">Finding Resolved</h5>
          <p className="text-sm text-green-700">
            This finding has been addressed and resolved by staff.
            {rule.fixed_at && (
              <span className="block text-xs text-green-600 mt-1">
                Resolved at: {new Date(rule.fixed_at).toLocaleString()}
              </span>
            )}
          </p>
        </div>
      )}

      {/* Evidence / Field Values — canonical UI fields only. Non-canonical
          keys are consumed by the rule engine but hidden from the reviewer
          view so evidence stays consistent across orgs. */}
      {fieldValues && (() => {
        const shown = Object.entries(fieldValues).filter(
          ([key, value]) => value && isCanonicalField(key)
        )
        if (shown.length === 0) return null
        return (
          <div className="p-4 rounded-lg border border-gray-200 bg-gray-50">
            <h5 className="text-sm font-medium text-gray-700 mb-3">Evidence (Field Values)</h5>
            <div className="grid grid-cols-2 gap-3">
              {shown.map(([key, value]) => (
                <div key={key}>
                  <dt className="text-xs text-gray-500 uppercase tracking-wide">
                    {FIELD_LABELS[key]}
                  </dt>
                  <dd className="text-sm text-gray-900 font-medium">{value}</dd>
                </div>
              ))}
            </div>
          </div>
        )
      })()}
    </div>
  )
}
