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

  enhanceNote: (orgId, note, ruleText) =>
    request(`/api/organizations/${orgId}/rules/enhance-note`, {
      method: 'POST',
      body: JSON.stringify({ note, rule_text: ruleText }),
    }),
}
