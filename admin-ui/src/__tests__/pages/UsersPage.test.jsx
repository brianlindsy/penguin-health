/**
 * Integration-style tests for UsersPage. Drives the page through MSW handlers
 * that round-trip through an in-memory store, so a created user shows up in
 * the table without further mocking.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { UsersPage } from '../../pages/UsersPage.jsx'
import { userPermStore, orgProgramsStore } from '../mocks/handlers.js'

// OrgWorkspaceLayout fetches /validation-runs and reads usePermissions; its
// network calls are handled by MSW. Stub window.confirm to bypass the
// destructive-action prompt the page raises on Remove.
beforeEach(() => {
  vi.spyOn(window, 'confirm').mockReturnValue(true)
})

// AuthProvider isn't wrapped here, so usePermissions reads useAuth via mock.
vi.mock('../../auth/useAuth.js', () => ({
  useAuth: () => ({ isSuperAdmin: true, permissions: null }),
}))

function renderAt(path = '/organizations/test-org/users') {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/organizations/:orgId/users" element={<UsersPage />} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('UsersPage', () => {
  it('shows the empty state when no users exist', async () => {
    renderAt()
    await waitFor(() => {
      expect(screen.getByText(/No users have been granted permissions/i)).toBeInTheDocument()
    })
  })

  it('lists existing users with role badges', async () => {
    userPermStore.put('test-org', 'alice@clinic.com', {
      role: 'org_admin',
      report_permissions: {},
      analytics_permissions: [],
    })
    userPermStore.put('test-org', 'bob@clinic.com', {
      role: 'member',
      report_permissions: { Billing: ['view'] },
      analytics_permissions: ['revenue_analysis'],
    })
    renderAt()
    expect(await screen.findByText('alice@clinic.com')).toBeInTheDocument()
    expect(screen.getByText('bob@clinic.com')).toBeInTheDocument()
    expect(screen.getByText('Org Admin')).toBeInTheDocument()
    expect(screen.getByText('Member')).toBeInTheDocument()
    expect(screen.getByText('Billing: view')).toBeInTheDocument()
    // Bob's row carries a "Revenue Analysis" chip; the sidebar nav link
    // shares the same text. Scope the assertion to the row.
    const bobRow = screen.getByText('bob@clinic.com').closest('tr')
    expect(within(bobRow).getByText('Revenue Analysis')).toBeInTheDocument()
  })

  it('creates a new user via the Add User modal', async () => {
    const user = userEvent.setup()
    renderAt()
    await screen.findByText(/No users have been granted permissions/i)

    await user.click(screen.getByRole('button', { name: 'Add User' }))
    await user.type(screen.getByPlaceholderText('user@example.com'), 'new@clinic.com')
    await user.click(screen.getByLabelText('Billing view'))
    await user.click(screen.getByRole('button', { name: 'Save' }))

    expect(await screen.findByText('new@clinic.com')).toBeInTheDocument()
    expect(screen.getByText('Billing: view')).toBeInTheDocument()
    expect(userPermStore.get('test-org', 'new@clinic.com').report_permissions.Billing)
      .toEqual(['view'])
  })

  it('disables category checkboxes when role is org_admin', async () => {
    const user = userEvent.setup()
    renderAt()
    await screen.findByText(/No users have been granted permissions/i)
    await user.click(screen.getByRole('button', { name: 'Add User' }))

    const billingView = screen.getByLabelText('Billing view')
    expect(billingView).not.toBeDisabled()

    await user.selectOptions(screen.getByRole('combobox'),
      'Org Admin — full access in this organization')

    expect(screen.getByLabelText('Billing view')).toBeDisabled()
  })

  it('removes a user when Remove is clicked', async () => {
    userPermStore.put('test-org', 'gone@clinic.com', {
      role: 'member',
      report_permissions: {},
      analytics_permissions: [],
    })
    const user = userEvent.setup()
    renderAt()
    expect(await screen.findByText('gone@clinic.com')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Remove' }))

    await waitFor(() => {
      expect(screen.queryByText('gone@clinic.com')).not.toBeInTheDocument()
    })
    expect(userPermStore.get('test-org', 'gone@clinic.com')).toBeNull()
  })

  describe('program permissions', () => {
    it('renders program checkboxes from the org list and saves the selection', async () => {
      orgProgramsStore.set('test-org', ['Program A', 'Program B'])
      const user = userEvent.setup()
      renderAt()
      await screen.findByText(/No users have been granted permissions/i)

      await user.click(screen.getByRole('button', { name: 'Add User' }))
      await user.type(screen.getByPlaceholderText('user@example.com'), 'prog@clinic.com')
      await user.click(screen.getByLabelText('Program Program A'))
      await user.click(screen.getByRole('button', { name: 'Save' }))

      await screen.findByText('prog@clinic.com')
      expect(userPermStore.get('test-org', 'prog@clinic.com').program_permissions)
        .toEqual(['Program A'])
    })

    it('shows the empty-list hint when the org has no programs configured yet', async () => {
      const user = userEvent.setup()
      renderAt()
      await screen.findByText(/No users have been granted permissions/i)
      await user.click(screen.getByRole('button', { name: 'Add User' }))

      expect(screen.getByText(/No programs are configured for this organization/i))
        .toBeInTheDocument()
    })

    it('summary row shows "All programs" when the user has no restrictions', async () => {
      userPermStore.put('test-org', 'unlimited@clinic.com', {
        role: 'member',
        report_permissions: { Billing: ['view'] },
        analytics_permissions: [],
        program_permissions: [],
      })
      renderAt()
      const row = (await screen.findByText('unlimited@clinic.com')).closest('tr')
      expect(within(row).getByText('All programs')).toBeInTheDocument()
    })

    it('adds a program through the Programs tab and persists it', async () => {
      const user = userEvent.setup()
      renderAt()
      await screen.findByText(/No users have been granted permissions/i)

      await user.click(screen.getByRole('button', { name: 'Programs' }))
      await user.type(screen.getByPlaceholderText(/e\.g\. Mental Health/i), 'Behavioral Health')
      await user.click(screen.getByRole('button', { name: 'Add' }))
      await user.click(screen.getByRole('button', { name: /Save programs/i }))

      await waitFor(() => {
        expect(orgProgramsStore.get('test-org')).toEqual(['Behavioral Health'])
      })
    })
  })
})
