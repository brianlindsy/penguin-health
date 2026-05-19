import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { SavedReportPage } from '../../pages/SavedReportPage.jsx'
import { ApiError } from '../../api/client.js'

vi.mock('../../api/client.js', async () => {
  const actual = await vi.importActual('../../api/client.js')
  return {
    ...actual,
    api: {
      getReport: vi.fn(),
      deleteReport: vi.fn(),
    },
  }
})

import { api } from '../../api/client.js'

let mockIsSuperAdmin = true
vi.mock('../../auth/usePermissions.js', () => ({
  usePermissions: () => ({ isSuperAdmin: mockIsSuperAdmin }),
}))

function renderAt(path = '/organizations/test-org/analytics/reports/rep-1') {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route
          path="/organizations/:orgId/analytics/reports/:reportId"
          element={<SavedReportPage />}
        />
      </Routes>
    </MemoryRouter>,
  )
}

const baseReport = {
  report_id: 'rep-1',
  name: 'Quarterly counts',
  organization_id: 'test-org',
  question: 'How many referrals last quarter?',
  explanation: 'Counts referrals from Q1.',
  viz_type: 'bar',
  mode: 'sql',
  columns: [{ name: 'total', type: 'bigint' }],
  rows: [['42']],
  row_count: 1,
  created_at: '2026-05-01T12:00:00Z',
  created_by: 'admin@example.com',
}

beforeEach(() => {
  vi.clearAllMocks()
  mockIsSuperAdmin = true
})

describe('SavedReportPage', () => {
  it('renders report with SQL and Delete for super admins', async () => {
    api.getReport.mockResolvedValue({ ...baseReport, sql: 'SELECT 1' })
    renderAt()
    expect(await screen.findByText('Quarterly counts')).toBeInTheDocument()
    expect(screen.getByText(/How many referrals last quarter/)).toBeInTheDocument()
    expect(screen.getByText(/Show generated SQL/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Delete/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Copy link/ })).toBeInTheDocument()
  })

  it('hides SQL and Delete for non-super-admin viewers, renders table viz', async () => {
    mockIsSuperAdmin = false
    api.getReport.mockResolvedValue({ ...baseReport, redacted: true })
    renderAt()
    expect(await screen.findByText('Quarterly counts')).toBeInTheDocument()
    expect(screen.queryByText(/Show generated SQL/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Delete/ })).not.toBeInTheDocument()
    // Table view shows the column header from the redacted payload.
    await waitFor(() => {
      expect(screen.getByText(/total/i)).toBeInTheDocument()
    })
  })

  it('shows access-denied message on 403', async () => {
    api.getReport.mockRejectedValue(new ApiError('Access denied', { status: 403 }))
    renderAt()
    expect(await screen.findByText(/don.{1,2}t have access/i)).toBeInTheDocument()
  })

  it('shows not-found message on 404', async () => {
    api.getReport.mockRejectedValue(new ApiError('Not found', { status: 404 }))
    renderAt()
    expect(await screen.findByText(/Report not found/i)).toBeInTheDocument()
  })
})
