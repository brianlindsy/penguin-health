/**
 * Tests for the usePermissions hook.
 *
 * Exercises every short-circuit path: super-admin, org-admin, member with
 * explicit grants, no record at all.
 */

import { describe, it, expect, vi } from 'vitest'
import { renderHook } from '@testing-library/react'
import { usePermissions } from '../../auth/usePermissions.js'

// Mock useAuth so we can plug different permission shapes in.
vi.mock('../../auth/AuthProvider.jsx', () => ({
  useAuth: vi.fn(),
}))

import { useAuth } from '../../auth/AuthProvider.jsx'

function setup(authValue) {
  useAuth.mockReturnValue(authValue)
  return renderHook(() => usePermissions())
}

describe('usePermissions', () => {
  describe('super admin', () => {
    it('grants every category × verb', () => {
      const { result } = setup({ isSuperAdmin: true, permissions: null })
      expect(result.current.isOrgAdmin).toBe(true)
      for (const cat of ['Intake', 'Billing', 'Compliance Audit', 'Quality Assurance']) {
        expect(result.current.canViewCategory(cat)).toBe(true)
        expect(result.current.canRunCategory(cat)).toBe(true)
      }
      expect(result.current.canViewAnalytics('staff_performance')).toBe(true)
      expect(result.current.canViewAnalytics('revenue_analysis')).toBe(true)
    })

    it('is ready immediately even without permissions payload', () => {
      const { result } = setup({ isSuperAdmin: true, permissions: null })
      expect(result.current.ready).toBe(true)
    })
  })

  describe('org admin', () => {
    it('grants every category × verb in their org', () => {
      const { result } = setup({
        isSuperAdmin: false,
        permissions: { role: 'org_admin', report_permissions: {}, analytics_permissions: [] },
      })
      expect(result.current.isOrgAdmin).toBe(true)
      expect(result.current.canViewCategory('Billing')).toBe(true)
      expect(result.current.canRunCategory('Compliance Audit')).toBe(true)
      expect(result.current.canViewAnalytics('staff_performance')).toBe(true)
    })
  })

  describe('member with explicit grants', () => {
    const authValue = {
      isSuperAdmin: false,
      permissions: {
        role: 'member',
        report_permissions: {
          Intake: ['view'],
          Billing: ['run'],
          'Compliance Audit': [],
          'Quality Assurance': ['view', 'run'],
        },
        analytics_permissions: ['revenue_analysis'],
      },
    }

    it('respects view-only grants', () => {
      const { result } = setup(authValue)
      expect(result.current.canViewCategory('Intake')).toBe(true)
      expect(result.current.canRunCategory('Intake')).toBe(false)
    })

    it('respects run-only grants', () => {
      const { result } = setup(authValue)
      expect(result.current.canRunCategory('Billing')).toBe(true)
      expect(result.current.canViewCategory('Billing')).toBe(false)
    })

    it('denies categories with empty verb list', () => {
      const { result } = setup(authValue)
      expect(result.current.canViewCategory('Compliance Audit')).toBe(false)
      expect(result.current.canRunCategory('Compliance Audit')).toBe(false)
    })

    it('builds viewable / runnable category sets', () => {
      const { result } = setup(authValue)
      expect(result.current.viewableCategories()).toEqual(
        new Set(['Intake', 'Quality Assurance']),
      )
      expect(result.current.runnableCategories()).toEqual(
        new Set(['Billing', 'Quality Assurance']),
      )
    })

    it('respects per-page analytics grants', () => {
      const { result } = setup(authValue)
      expect(result.current.canViewAnalytics('revenue_analysis')).toBe(true)
      expect(result.current.canViewAnalytics('staff_performance')).toBe(false)
    })
  })

  describe('no permission record', () => {
    it('denies everything', () => {
      const { result } = setup({ isSuperAdmin: false, permissions: null })
      expect(result.current.ready).toBe(false)
      expect(result.current.isOrgAdmin).toBe(false)
      expect(result.current.canViewCategory('Billing')).toBe(false)
      expect(result.current.canRunCategory('Billing')).toBe(false)
      expect(result.current.canViewAnalytics('staff_performance')).toBe(false)
      expect(result.current.viewableCategories()).toEqual(new Set())
      expect(result.current.runnableCategories()).toEqual(new Set())
    })
  })
})
