/**
 * Verifies the REVENUE AT RISK summary card is only visible to users with
 * org-admin or the revenue_analysis analytics permission. Everyone else sees
 * the other three cards but not the revenue one.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { DocumentQueuePage } from '../../pages/DocumentQueuePage.jsx'

vi.mock('../../auth/useAuth.js', () => ({
  useAuth: vi.fn(),
}))

import { useAuth } from '../../auth/useAuth.js'

function setAuth(value) {
  useAuth.mockReturnValue(value)
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/organizations/test-org/document-queue']}>
      <Routes>
        <Route
          path="/organizations/:orgId/document-queue"
          element={<DocumentQueuePage />}
        />
      </Routes>
    </MemoryRouter>,
  )
}

async function waitForCards() {
  await waitFor(() => {
    expect(screen.getByText('NEEDS ACTION')).toBeInTheDocument()
  })
}

describe('DocumentQueuePage revenue-at-risk card gating', () => {
  beforeEach(() => {
    useAuth.mockReset()
  })

  it('shows the card for org admins', async () => {
    setAuth({
      isSuperAdmin: false,
      permissions: { role: 'org_admin', report_permissions: {}, analytics_permissions: [] },
    })
    renderPage()
    await waitForCards()
    expect(screen.getByText('REVENUE AT RISK')).toBeInTheDocument()
  })

  it('shows the card for members with revenue_analysis permission', async () => {
    setAuth({
      isSuperAdmin: false,
      permissions: {
        role: 'member',
        report_permissions: {},
        analytics_permissions: ['revenue_analysis'],
      },
    })
    renderPage()
    await waitForCards()
    expect(screen.getByText('REVENUE AT RISK')).toBeInTheDocument()
  })

  it('hides the card for members without revenue_analysis permission', async () => {
    setAuth({
      isSuperAdmin: false,
      permissions: {
        role: 'member',
        report_permissions: {},
        analytics_permissions: ['staff_performance'],
      },
    })
    renderPage()
    await waitForCards()
    expect(screen.queryByText('REVENUE AT RISK')).not.toBeInTheDocument()
    // Other summary cards remain visible.
    expect(screen.getByText('AWAITING STAFF')).toBeInTheDocument()
    expect(screen.getByText('PASSED')).toBeInTheDocument()
    expect(screen.getByText('CONFIRMED')).toBeInTheDocument()
  })
})
