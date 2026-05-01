/**
 * Tests for the RoleGuard component.
 *
 * Verifies it renders children when each kind of requirement is met, falls
 * back when denied, and renders nothing while permissions are still loading.
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { RoleGuard } from '../../auth/RoleGuard.jsx'

vi.mock('../../auth/AuthProvider.jsx', () => ({
  useAuth: vi.fn(),
}))

import { useAuth } from '../../auth/AuthProvider.jsx'

function setAuth(value) {
  useAuth.mockReturnValue(value)
}

describe('RoleGuard', () => {
  it('renders nothing while permissions are still loading', () => {
    setAuth({ isSuperAdmin: false, permissions: null })
    const { container } = render(
      <RoleGuard requireOrgAdmin>
        <div>secret</div>
      </RoleGuard>,
    )
    expect(container).toBeEmptyDOMElement()
  })

  it('lets super-admins through every gate immediately', () => {
    setAuth({ isSuperAdmin: true, permissions: null })
    render(
      <RoleGuard requireSuperAdmin>
        <div>secret</div>
      </RoleGuard>,
    )
    expect(screen.getByText('secret')).toBeInTheDocument()
  })

  it('renders fallback when org-admin requirement fails', () => {
    setAuth({
      isSuperAdmin: false,
      permissions: { role: 'member', report_permissions: {}, analytics_permissions: [] },
    })
    render(
      <RoleGuard requireOrgAdmin fallback={<div>denied</div>}>
        <div>secret</div>
      </RoleGuard>,
    )
    expect(screen.getByText('denied')).toBeInTheDocument()
    expect(screen.queryByText('secret')).not.toBeInTheDocument()
  })

  it('respects requireAnalytics', () => {
    setAuth({
      isSuperAdmin: false,
      permissions: {
        role: 'member',
        report_permissions: {},
        analytics_permissions: ['revenue_analysis'],
      },
    })
    render(
      <RoleGuard requireAnalytics="revenue_analysis" fallback={<div>denied</div>}>
        <div>shown</div>
      </RoleGuard>,
    )
    expect(screen.getByText('shown')).toBeInTheDocument()
  })

  it('blocks requireAnalytics when page is not granted', () => {
    setAuth({
      isSuperAdmin: false,
      permissions: {
        role: 'member',
        report_permissions: {},
        analytics_permissions: ['staff_performance'],
      },
    })
    render(
      <RoleGuard requireAnalytics="revenue_analysis" fallback={<div>denied</div>}>
        <div>shown</div>
      </RoleGuard>,
    )
    expect(screen.getByText('denied')).toBeInTheDocument()
  })

  it('respects requireViewCategory and requireRunCategory independently', () => {
    setAuth({
      isSuperAdmin: false,
      permissions: {
        role: 'member',
        report_permissions: { Billing: ['view'] },
        analytics_permissions: [],
      },
    })
    const { rerender } = render(
      <RoleGuard requireViewCategory="Billing" fallback={<div>denied</div>}>
        <div>shown</div>
      </RoleGuard>,
    )
    expect(screen.getByText('shown')).toBeInTheDocument()

    rerender(
      <RoleGuard requireRunCategory="Billing" fallback={<div>denied</div>}>
        <div>shown</div>
      </RoleGuard>,
    )
    expect(screen.getByText('denied')).toBeInTheDocument()
  })
})
