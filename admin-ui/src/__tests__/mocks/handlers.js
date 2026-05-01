/**
 * MSW (Mock Service Worker) handlers for API mocking.
 *
 * These handlers intercept network requests and return mock responses.
 * Used for integration-style tests that need realistic API behavior.
 */

import { http, HttpResponse } from 'msw'

// Sample data
const mockOrganizations = [
  {
    organization_id: 'test-org',
    organization_name: 'Test Organization',
    enabled: true,
    s3_bucket_name: 'penguin-health-test-org',
    created_at: '2024-01-01T00:00:00Z',
  },
  {
    organization_id: 'other-org',
    organization_name: 'Other Organization',
    enabled: true,
    s3_bucket_name: 'penguin-health-other-org',
    created_at: '2024-01-02T00:00:00Z',
  },
]

const mockRules = [
  {
    rule_id: 'rule-001',
    name: 'Service Date Documentation',
    category: 'Compliance',
    enabled: true,
    type: 'llm',
    rule_text: 'Verify the service date is documented.',
  },
  {
    rule_id: 'rule-002',
    name: 'Consumer Signature',
    category: 'Compliance',
    enabled: true,
    type: 'llm',
    rule_text: 'Verify the consumer signature is present.',
  },
]

const mockValidationRuns = [
  {
    validation_run_id: '20240115-100000',
    timestamp: '2024-01-15T10:00:00Z',
    total_documents: 10,
    passed: 8,
    failed: 2,
    skipped: 0,
  },
  {
    validation_run_id: '20240114-090000',
    timestamp: '2024-01-14T09:00:00Z',
    total_documents: 15,
    passed: 12,
    failed: 3,
    skipped: 0,
  },
]

export const handlers = [
  // Current user's permission record (defaults to super-admin so existing
  // tests behave as before).
  http.get('*/api/me/permissions', () => {
    return HttpResponse.json({
      is_super_admin: true,
      role: null,
      organization_id: null,
      report_permissions: {
        Intake: ['view', 'run'],
        Billing: ['view', 'run'],
        'Compliance Audit': ['view', 'run'],
        'Quality Assurance': ['view', 'run'],
      },
      analytics_permissions: ['staff_performance', 'revenue_analysis'],
    })
  }),

  // List organizations
  http.get('*/api/organizations', () => {
    return HttpResponse.json({
      organizations: mockOrganizations,
    })
  }),

  // Get organization
  http.get('*/api/organizations/:orgId', ({ params }) => {
    const org = mockOrganizations.find(o => o.organization_id === params.orgId)
    if (!org) {
      return HttpResponse.json({ error: 'Organization not found' }, { status: 404 })
    }
    return HttpResponse.json(org)
  }),

  // List rules
  http.get('*/api/organizations/:orgId/rules', () => {
    return HttpResponse.json({
      rules: mockRules,
      count: mockRules.length,
    })
  }),

  // Get rule
  http.get('*/api/organizations/:orgId/rules/:ruleId', ({ params }) => {
    const rule = mockRules.find(r => r.rule_id === params.ruleId)
    if (!rule) {
      return HttpResponse.json({ error: 'Rule not found' }, { status: 404 })
    }
    return HttpResponse.json(rule)
  }),

  // Create rule
  http.post('*/api/organizations/:orgId/rules', async ({ request }) => {
    const body = await request.json()
    return HttpResponse.json({
      rule_id: body.id,
      ...body,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }, { status: 201 })
  }),

  // Update rule
  http.put('*/api/organizations/:orgId/rules/:ruleId', async ({ params, request }) => {
    const body = await request.json()
    return HttpResponse.json({
      rule_id: params.ruleId,
      ...body,
      updated_at: new Date().toISOString(),
    })
  }),

  // List validation runs
  http.get('*/api/organizations/:orgId/validation-runs', () => {
    return HttpResponse.json({
      runs: mockValidationRuns,
      count: mockValidationRuns.length,
    })
  }),

  // Get validation run
  http.get('*/api/organizations/:orgId/validation-runs/:runId', ({ params }) => {
    const run = mockValidationRuns.find(r => r.validation_run_id === params.runId)
    if (!run) {
      return HttpResponse.json({ error: 'Validation run not found' }, { status: 404 })
    }
    return HttpResponse.json({
      ...run,
      documents: [
        {
          document_id: '12345',
          status: 'FAIL',
          rules: [{ rule_id: 'rule-001', status: 'FAIL', message: 'Service date not found' }],
        },
      ],
    })
  }),

  // Trigger validation run
  http.post('*/api/organizations/:orgId/validation-runs', () => {
    return HttpResponse.json({
      message: 'Validation run started',
      validation_run_id: `${Date.now()}`,
    })
  }),

  // Confirm finding
  http.put('*/api/organizations/:orgId/validation-runs/:runId/documents/:docId/confirm-finding', () => {
    return HttpResponse.json({
      finding_confirmed: true,
      finding_confirmed_at: new Date().toISOString(),
      finding_confirmed_by: 'test@example.com',
    })
  }),

  // Mark resolved
  http.put('*/api/organizations/:orgId/validation-runs/:runId/documents/:docId/mark-resolved', () => {
    return HttpResponse.json({
      fixed: true,
      fixed_at: new Date().toISOString(),
      fixed_by: 'test@example.com',
    })
  }),

  // Mark incorrect
  http.put('*/api/organizations/:orgId/validation-runs/:runId/documents/:docId/mark-incorrect', () => {
    return HttpResponse.json({
      feedback_given: true,
      feedback_given_at: new Date().toISOString(),
      feedback_given_by: 'test@example.com',
    })
  }),

  // ---- User permissions CRUD ----
  http.get('*/api/organizations/:orgId/users', ({ params }) => {
    const list = userPermStore.list(params.orgId)
    return HttpResponse.json({ users: list, count: list.length })
  }),

  http.get('*/api/organizations/:orgId/users/:email', ({ params }) => {
    const email = decodeURIComponent(params.email)
    const user = userPermStore.get(params.orgId, email)
    if (!user) return HttpResponse.json({ error: 'Not found' }, { status: 404 })
    return HttpResponse.json(user)
  }),

  http.put('*/api/organizations/:orgId/users/:email', async ({ params, request }) => {
    const email = decodeURIComponent(params.email)
    const body = await request.json()
    const existing = userPermStore.get(params.orgId, email)
    const item = userPermStore.put(params.orgId, email, body)
    return HttpResponse.json(item, { status: existing ? 200 : 201 })
  }),

  http.delete('*/api/organizations/:orgId/users/:email', ({ params }) => {
    const email = decodeURIComponent(params.email)
    userPermStore.remove(params.orgId, email)
    // Backend returns 204 with body '{}' (its response() helper json-encodes
    // empty dicts), so the client's await res.json() succeeds.
    return HttpResponse.json({}, { status: 200 })
  }),
]

// In-memory store so tests can round-trip create → list → edit → delete.
// Reset between tests by calling resetUserPermStore() in test setup.
const _userPermData = new Map() // key: `${orgId}::${email}`
export const userPermStore = {
  reset() { _userPermData.clear() },
  list(orgId) {
    return [..._userPermData.entries()]
      .filter(([k]) => k.startsWith(`${orgId}::`))
      .map(([, v]) => v)
  },
  get(orgId, email) {
    return _userPermData.get(`${orgId}::${email}`) || null
  },
  put(orgId, email, body) {
    const item = {
      email,
      organization_id: orgId,
      role: body.role || 'member',
      report_permissions: body.report_permissions || {},
      analytics_permissions: body.analytics_permissions || [],
      created_at: '2024-01-01T00:00:00Z',
      updated_at: new Date().toISOString(),
    }
    _userPermData.set(`${orgId}::${email}`, item)
    return item
  },
  remove(orgId, email) {
    _userPermData.delete(`${orgId}::${email}`)
  },
}

export function resetUserPermStore() {
  userPermStore.reset()
}
