/**
 * Tests for RootLanding — the "/" landing component.
 *
 * Users whose Cognito profile carries a `custom:organization_id` land on
 * their org's document queue. Everyone else (super-admins / unassigned)
 * lands on the org picker.
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'

vi.mock('../auth/useAuth.js', () => ({
  useAuth: vi.fn(),
}))

vi.mock('../pages/OrganizationsPage.jsx', () => ({
  OrganizationsPage: () => <div>Org Picker</div>,
}))

import { useAuth } from '../auth/useAuth.js'
import { RootLanding } from '../App.jsx'

function renderAt(path) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/" element={<RootLanding />} />
        <Route
          path="/organizations/:orgId/document-queue"
          element={<div>Document Queue</div>}
        />
      </Routes>
    </MemoryRouter>
  )
}

describe('RootLanding', () => {
  it('redirects to org document queue when organizationId is present in claims', () => {
    useAuth.mockReturnValue({
      userClaims: { organizationId: 'org-abc', isSuperAdmin: false, groups: [] },
    })

    renderAt('/')

    expect(screen.getByText('Document Queue')).toBeInTheDocument()
    expect(screen.queryByText('Org Picker')).not.toBeInTheDocument()
  })

  it('renders the org picker when no organizationId is present', () => {
    useAuth.mockReturnValue({
      userClaims: { organizationId: null, isSuperAdmin: true, groups: ['Admins'] },
    })

    renderAt('/')

    expect(screen.getByText('Org Picker')).toBeInTheDocument()
  })
})
