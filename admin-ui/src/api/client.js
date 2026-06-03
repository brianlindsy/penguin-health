const API_BASE = import.meta.env.VITE_API_URL || ''

let getTokenFn = null
let onUnauthorizedFn = null

export function setTokenProvider(fn) {
  getTokenFn = fn
}

export function setOnUnauthorized(fn) {
  onUnauthorizedFn = fn
}

// The analytics endpoints return structured failures: { error, code, sql }.
// The plain Error type loses `code` and `sql`, which the UI needs to render
// validation rejections nicely. Surface them via this subclass.
export class ApiError extends Error {
  constructor(message, { status, code, sql } = {}) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.sql = sql
  }
}

async function request(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...options.headers }

  if (getTokenFn) {
    try {
      const token = await getTokenFn()
      headers['Authorization'] = `Bearer ${token}`
    } catch {
      if (onUnauthorizedFn) onUnauthorizedFn()
      throw new Error('Authentication required')
    }
  }

  const res = await fetch(`${API_BASE}${path}`, { ...options, headers })

  if (res.status === 401) {
    if (onUnauthorizedFn) onUnauthorizedFn()
    throw new Error('Unauthorized')
  }

  // 204 No Content (used by DELETEs) has no body to parse.
  if (res.status === 204) {
    if (!res.ok) throw new Error(`Request failed: ${res.status}`)
    return null
  }

  const data = await res.json()

  if (!res.ok) {
    throw new ApiError(data.error || `Request failed: ${res.status}`, {
      status: res.status,
      code: data.code,
      sql: data.sql,
    })
  }

  return data
}

export const api = {
  listOrganizations: () => request('/api/organizations'),

  getOrganization: (orgId) => request(`/api/organizations/${orgId}`),

  listRules: (orgId) => request(`/api/organizations/${orgId}/rules`),

  getRule: (orgId, ruleId) => request(`/api/organizations/${orgId}/rules/${ruleId}`),

  createRule: (orgId, rule) =>
    request(`/api/organizations/${orgId}/rules`, {
      method: 'POST',
      body: JSON.stringify(rule),
    }),

  updateRule: (orgId, ruleId, rule) =>
    request(`/api/organizations/${orgId}/rules/${ruleId}`, {
      method: 'PUT',
      body: JSON.stringify(rule),
    }),

  getRulesConfig: (orgId) => request(`/api/organizations/${orgId}/rules-config`),

  updateRulesConfig: (orgId, config) =>
    request(`/api/organizations/${orgId}/rules-config`, {
      method: 'PUT',
      body: JSON.stringify(config),
    }),

  enhanceRuleFields: (orgId, ruleText) =>
    request(`/api/organizations/${orgId}/rules/enhance-fields`, {
      method: 'POST',
      body: JSON.stringify({ rule_text: ruleText }),
    }),

  enhanceNote: (orgId, note, ruleText, documentId, validationRunId, ruleId, notes) =>
    request(`/api/organizations/${orgId}/rules/enhance-note`, {
      method: 'POST',
      body: JSON.stringify({
        note,
        rule_text: ruleText,
        document_id: documentId,
        validation_run_id: validationRunId,
        rule_id: ruleId,
        notes,
      }),
    }),

  // Validation Results
  listValidationRuns: (orgId, { since, until, includeDetails, slim, limit } = {}) => {
    const qs = new URLSearchParams()
    if (since) qs.set('since', since)
    if (until) qs.set('until', until)
    if (includeDetails) qs.set('include', 'details')
    if (slim) qs.set('slim', 'true')
    if (limit != null) qs.set('limit', String(limit))
    const suffix = qs.toString() ? `?${qs}` : ''
    return request(`/api/organizations/${orgId}/validation-runs${suffix}`)
  },

  triggerValidationRun: (orgId, categories, dates) => {
    const body = {}
    if (categories && categories.length) body.categories = categories
    if (dates && dates.length) body.dates = dates
    return request(`/api/organizations/${orgId}/validation-runs`, {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  // RBAC
  getMyPermissions: () => request('/api/me/permissions'),

  listOrgUsers: (orgId) =>
    request(`/api/organizations/${orgId}/users`),

  getOrgUser: (orgId, email) =>
    request(`/api/organizations/${orgId}/users/${encodeURIComponent(email)}`),

  upsertOrgUser: (orgId, email, payload) =>
    request(`/api/organizations/${orgId}/users/${encodeURIComponent(email)}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    }),

  deleteOrgUser: (orgId, email) =>
    request(`/api/organizations/${orgId}/users/${encodeURIComponent(email)}`, {
      method: 'DELETE',
    }),

  getValidationRun: (orgId, runId) =>
    request(`/api/organizations/${orgId}/validation-runs/${runId}`),

  getValidationResult: (orgId, runId, docId) =>
    request(`/api/organizations/${orgId}/validation-runs/${runId}/documents/${docId}`),

  confirmFinding: (orgId, runId, docId, ruleId) =>
    request(`/api/organizations/${orgId}/validation-runs/${runId}/documents/${docId}/confirm-finding`, {
      method: 'PUT',
      body: JSON.stringify({ rule_id: ruleId }),
    }),

  markResolved: (orgId, runId, docId, ruleId) =>
    request(`/api/organizations/${orgId}/validation-runs/${runId}/documents/${docId}/mark-resolved`, {
      method: 'PUT',
      body: JSON.stringify({ rule_id: ruleId }),
    }),

  markIncorrect: (orgId, runId, docId, ruleId) =>
    request(`/api/organizations/${orgId}/validation-runs/${runId}/documents/${docId}/mark-incorrect`, {
      method: 'PUT',
      body: JSON.stringify({ rule_id: ruleId }),
    }),

  // Analytics: NL query
  nlQuery: (orgId, question) =>
    request(`/api/organizations/${orgId}/analytics/nl-query`, {
      method: 'POST',
      body: JSON.stringify({ question }),
    }),

  // Kicks off an async deep-analysis job. Returns { job_id, status:'running',
  // total_rows, done_rows, sql }. Poll getDeepJob until status is terminal.
  startDeepJob: (orgId, question, scopeSql) =>
    request(`/api/organizations/${orgId}/analytics/nl-query/deep`, {
      method: 'POST',
      body: JSON.stringify({ question, scope_sql: scopeSql }),
    }),

  getDeepJob: (orgId, jobId) =>
    request(`/api/organizations/${orgId}/analytics/nl-query/deep/${jobId}`),

  // Analytics: Saved Reports
  saveReport: (orgId, report) =>
    request(`/api/organizations/${orgId}/analytics/reports`, {
      method: 'POST',
      body: JSON.stringify(report),
    }),

  listReports: (orgId) =>
    request(`/api/organizations/${orgId}/analytics/reports`),

  getReport: (orgId, reportId) =>
    request(`/api/organizations/${orgId}/analytics/reports/${reportId}`),

  deleteReport: (orgId, reportId) =>
    request(`/api/organizations/${orgId}/analytics/reports/${reportId}`, {
      method: 'DELETE',
    }),

  // Eligibility (Stedi)
  verifyEligibility: (orgId, input) =>
    request(`/api/organizations/${orgId}/eligibility/verify`, {
      method: 'POST',
      body: JSON.stringify(input),
    }),

  getEligibilityHistory: (orgId, { first, last, dob, limit = 20 }) => {
    const qs = new URLSearchParams({ first, last, dob, limit: String(limit) })
    return request(`/api/organizations/${orgId}/eligibility/history?${qs}`)
  },

  getEligibilityConfig: (orgId) =>
    request(`/api/organizations/${orgId}/eligibility/config`),

  updateEligibilityConfig: (orgId, body) =>
    request(`/api/organizations/${orgId}/eligibility/config`, {
      method: 'PUT',
      body: JSON.stringify(body),
    }),

  // Eligibility worklist (encounter-keyed; populated by the FHIR poller).
  listEligibilityEncounters: (orgId, { limit = 100 } = {}) =>
    request(`/api/organizations/${orgId}/eligibility/encounters?limit=${limit}`),

  resolveEligibilityEncounter: (orgId, encounterId, body) =>
    request(
      `/api/organizations/${orgId}/eligibility/encounters/${encodeURIComponent(encounterId)}/resolve`,
      { method: 'PUT', body: JSON.stringify(body) },
    ),

  rerunEligibilityEncounter: (orgId, encounterId, demographics) =>
    request(
      `/api/organizations/${orgId}/eligibility/encounters/${encodeURIComponent(encounterId)}/rerun`,
      { method: 'POST', body: JSON.stringify(demographics) },
    ),
}
