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
]
