/**
 * Tests for API client.
 *
 * Tests the API request handling including:
 * - Authorization header injection
 * - 401 handling and onUnauthorized callback
 * - Error message extraction
 * - Request methods (GET, POST, PUT)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { api, setTokenProvider, setOnUnauthorized } from '../../api/client.js'

describe('API Client', () => {
  let fetchSpy

  beforeEach(() => {
    fetchSpy = vi.spyOn(global, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
    setTokenProvider(null)
    setOnUnauthorized(null)
  })

  it('includes Authorization header when token provider is set', async () => {
    const mockToken = 'mock-jwt-token'
    setTokenProvider(() => Promise.resolve(mockToken))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ organizations: [] }),
    })

    await api.listOrganizations()

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: `Bearer ${mockToken}`,
        }),
      })
    )
  })

  it('calls onUnauthorized callback on 401 response', async () => {
    const onUnauthorized = vi.fn()
    setTokenProvider(() => Promise.resolve('token'))
    setOnUnauthorized(onUnauthorized)

    fetchSpy.mockResolvedValueOnce({
      ok: false,
      status: 401,
      json: () => Promise.resolve({ error: 'Unauthorized' }),
    })

    await expect(api.listOrganizations()).rejects.toThrow('Unauthorized')
    expect(onUnauthorized).toHaveBeenCalled()
  })

  it('throws error with message from API response', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: () => Promise.resolve({ error: 'Invalid request data' }),
    })

    await expect(api.listOrganizations()).rejects.toThrow('Invalid request data')
  })

  it('throws error with status code when no error message', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve({}),
    })

    await expect(api.listOrganizations()).rejects.toThrow('500')
  })

  it('calls onUnauthorized when token provider fails', async () => {
    const onUnauthorized = vi.fn()
    setTokenProvider(() => Promise.reject(new Error('Token expired')))
    setOnUnauthorized(onUnauthorized)

    await expect(api.listOrganizations()).rejects.toThrow('Authentication required')
    expect(onUnauthorized).toHaveBeenCalled()
  })

  it('creates rule with correct request body', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 201,
      json: () => Promise.resolve({ rule_id: 'new-rule' }),
    })

    const rule = {
      id: 'new-rule',
      name: 'Test',
      category: 'Compliance',
      rule_text: 'Check documentation.',
    }
    await api.createRule('org-123', rule)

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining('/api/organizations/org-123/rules'),
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify(rule),
      })
    )
  })

  it('updates rule with correct request body', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ rule_id: 'rule-001' }),
    })

    const rule = { name: 'Updated Rule' }
    await api.updateRule('org-123', 'rule-001', rule)

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining('/api/organizations/org-123/rules/rule-001'),
      expect.objectContaining({
        method: 'PUT',
        body: JSON.stringify(rule),
      })
    )
  })

  it('confirms finding with correct request body', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ finding_confirmed: true }),
    })

    await api.confirmFinding('org-123', 'run-001', 'doc-001', 'rule-001')

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining('/api/organizations/org-123/validation-runs/run-001/documents/doc-001/confirm-finding'),
      expect.objectContaining({
        method: 'PUT',
        body: JSON.stringify({ rule_id: 'rule-001' }),
      })
    )
  })

  it('triggers validation run with POST request', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ validation_run_id: 'new-run-id' }),
    })

    await api.triggerValidationRun('org-123')

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining('/api/organizations/org-123/validation-runs'),
      expect.objectContaining({
        method: 'POST',
      })
    )
  })

  it('forwards categories[] to triggerValidationRun', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 202,
      json: () => Promise.resolve({ validation_run_id: 'new-run-id' }),
    })

    await api.triggerValidationRun('org-123', ['Billing', 'Intake'])

    const call = fetchSpy.mock.calls[0]
    expect(call[1].method).toBe('POST')
    expect(JSON.parse(call[1].body)).toEqual({ categories: ['Billing', 'Intake'] })
  })

  it('omits categories from triggerValidationRun body when none supplied', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 202,
      json: () => Promise.resolve({}),
    })

    await api.triggerValidationRun('org-123')
    expect(JSON.parse(fetchSpy.mock.calls[0][1].body)).toEqual({})
  })

  it('forwards dates[] to triggerValidationRun', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 202,
      json: () => Promise.resolve({}),
    })

    await api.triggerValidationRun('org-123', ['Billing'], ['2026-05-12', '2026-05-13'])

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body)
    expect(body.dates).toEqual(['2026-05-12', '2026-05-13'])
    expect(body.categories).toEqual(['Billing'])
  })

  it('omits dates when empty', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 202,
      json: () => Promise.resolve({}),
    })

    await api.triggerValidationRun('org-123', ['Billing'], [])
    const body = JSON.parse(fetchSpy.mock.calls[0][1].body)
    expect(body.dates).toBeUndefined()
  })

  it('getMyPermissions hits /api/me/permissions', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ is_super_admin: false }),
    })

    const result = await api.getMyPermissions()

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining('/api/me/permissions'),
      expect.any(Object),
    )
    expect(result).toEqual({ is_super_admin: false })
  })

  it('returns parsed JSON data on success', async () => {
    setTokenProvider(() => Promise.resolve('token'))

    const expectedData = {
      organizations: [
        { organization_id: 'org-1', organization_name: 'Org 1' },
        { organization_id: 'org-2', organization_name: 'Org 2' },
      ],
    }

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve(expectedData),
    })

    const result = await api.listOrganizations()

    expect(result).toEqual(expectedData)
  })

  it('works without token provider for public endpoints', async () => {
    // No token provider set
    setTokenProvider(null)

    fetchSpy.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ status: 'ok' }),
    })

    // This will work but won't have Authorization header
    // In practice, all endpoints require auth, but this tests the code path
    const result = await api.listOrganizations()

    expect(result).toEqual({ status: 'ok' })
    expect(fetchSpy).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.not.objectContaining({
          Authorization: expect.any(String),
        }),
      })
    )
  })
})
