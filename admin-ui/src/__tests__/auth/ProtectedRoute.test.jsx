/**
 * Tests for ProtectedRoute component.
 *
 * Tests route protection including:
 * - Loading state display
 * - Redirect to login when not authenticated
 * - Render children when authenticated
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'

// Mock the AuthProvider
vi.mock('../../auth/AuthProvider.jsx', () => ({
  useAuth: vi.fn(),
}))

import { useAuth } from '../../auth/AuthProvider.jsx'
import { ProtectedRoute } from '../../auth/ProtectedRoute.jsx'

describe('ProtectedRoute', () => {
  it('shows loading state when auth is loading', () => {
    useAuth.mockReturnValue({ user: null, loading: true })

    render(
      <MemoryRouter initialEntries={['/protected']}>
        <Routes>
          <Route path="/protected" element={
            <ProtectedRoute>
              <div>Protected Content</div>
            </ProtectedRoute>
          } />
        </Routes>
      </MemoryRouter>
    )

    expect(screen.getByText('Loading...')).toBeInTheDocument()
  })

  it('redirects to login when no user', () => {
    useAuth.mockReturnValue({ user: null, loading: false })

    render(
      <MemoryRouter initialEntries={['/protected']}>
        <Routes>
          <Route path="/login" element={<div>Login Page</div>} />
          <Route path="/protected" element={
            <ProtectedRoute>
              <div>Protected Content</div>
            </ProtectedRoute>
          } />
        </Routes>
      </MemoryRouter>
    )

    expect(screen.getByText('Login Page')).toBeInTheDocument()
    expect(screen.queryByText('Protected Content')).not.toBeInTheDocument()
  })

  it('renders children when user is authenticated', () => {
    useAuth.mockReturnValue({
      user: { email: 'test@test.com', token: 'token' },
      loading: false,
    })

    render(
      <MemoryRouter initialEntries={['/protected']}>
        <Routes>
          <Route path="/protected" element={
            <ProtectedRoute>
              <div>Protected Content</div>
            </ProtectedRoute>
          } />
        </Routes>
      </MemoryRouter>
    )

    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })

  it('renders nested children correctly', () => {
    useAuth.mockReturnValue({
      user: { email: 'test@test.com', token: 'token' },
      loading: false,
    })

    render(
      <MemoryRouter initialEntries={['/protected']}>
        <Routes>
          <Route path="/protected" element={
            <ProtectedRoute>
              <div>
                <h1>Dashboard</h1>
                <p>Welcome to the dashboard</p>
              </div>
            </ProtectedRoute>
          } />
        </Routes>
      </MemoryRouter>
    )

    expect(screen.getByText('Dashboard')).toBeInTheDocument()
    expect(screen.getByText('Welcome to the dashboard')).toBeInTheDocument()
  })
})
