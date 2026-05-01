const API_BASE = import.meta.env.VITE_API_URL || ''

let getTokenFn = null
let onUnauthorizedFn = null

export function setTokenProvider(fn) {
  getTokenFn = fn
}

export function setOnUnauthorized(fn) {
  onUnauthorizedFn = fn
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
    throw new Error(data.error || `Request failed: ${res.status}`)
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
  listValidationRuns: (orgId) =>
    request(`/api/organizations/${orgId}/validation-runs`),

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
}
