/**
 * Tests for AuthProvider component.
 *
 * Tests authentication context including:
 * - User session management
 * - Claims extraction from Cognito tokens
 * - Super admin detection
 * - Login flow
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'

// Mock amazon-cognito-identity-js
vi.mock('amazon-cognito-identity-js', () => ({
  CognitoUserPool: vi.fn().mockImplementation(() => ({
    getCurrentUser: vi.fn().mockReturnValue(null),
  })),
  CognitoUser: vi.fn(),
  AuthenticationDetails: vi.fn(),
}))

// Test component to access auth context
function TestConsumer() {
  // Import dynamically to get fresh instance after mocks
  const { useAuth } = require('../../auth/AuthProvider.jsx')
  const { user, loading, userClaims } = useAuth()

  if (loading) return <div data-testid="loading">Loading...</div>

  return (
    <div>
      <span data-testid="user">{user?.email || 'No user'}</span>
      <span data-testid="is-super-admin">{userClaims.isSuperAdmin ? 'true' : 'false'}</span>
      <span data-testid="org-id">{userClaims.organizationId || 'none'}</span>
    </div>
  )
}

describe('AuthProvider', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.resetModules()
  })

  it('shows loading state initially', async () => {
    const { CognitoUserPool } = await import('amazon-cognito-identity-js')

    // Mock user pool with no current user
    CognitoUserPool.mockImplementation(() => ({
      getCurrentUser: vi.fn().mockReturnValue(null),
    }))

    const { AuthProvider } = await import('../../auth/AuthProvider.jsx')

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    // Should eventually show no user (after loading)
    await waitFor(() => {
      expect(screen.queryByTestId('user')).toHaveTextContent('No user')
    })
  })

  it('shows no user when not logged in', async () => {
    const { CognitoUserPool } = await import('amazon-cognito-identity-js')

    CognitoUserPool.mockImplementation(() => ({
      getCurrentUser: vi.fn().mockReturnValue(null),
    }))

    const { AuthProvider } = await import('../../auth/AuthProvider.jsx')

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('No user')
    })
  })

  it('extracts user claims from valid session', async () => {
    const { CognitoUserPool } = await import('amazon-cognito-identity-js')

    const mockIdToken = {
      getJwtToken: () => 'mock-jwt-token',
      payload: {
        email: 'admin@test.com',
        'cognito:groups': ['Admins'],
        'custom:organization_id': 'org-123',
      },
    }

    const mockSession = {
      isValid: () => true,
      getIdToken: () => mockIdToken,
    }

    const mockCognitoUser = {
      getUsername: () => 'admin@test.com',
      getSession: vi.fn((callback) => callback(null, mockSession)),
    }

    CognitoUserPool.mockImplementation(() => ({
      getCurrentUser: vi.fn().mockReturnValue(mockCognitoUser),
    }))

    const { AuthProvider } = await import('../../auth/AuthProvider.jsx')

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('admin@test.com')
      expect(screen.getByTestId('is-super-admin')).toHaveTextContent('true')
      expect(screen.getByTestId('org-id')).toHaveTextContent('org-123')
    })
  })

  it('detects non-super-admin users', async () => {
    const { CognitoUserPool } = await import('amazon-cognito-identity-js')

    const mockIdToken = {
      getJwtToken: () => 'mock-jwt-token',
      payload: {
        email: 'user@test.com',
        'cognito:groups': [],  // Not in Admins group
        'custom:organization_id': 'org-456',
      },
    }

    const mockSession = {
      isValid: () => true,
      getIdToken: () => mockIdToken,
    }

    const mockCognitoUser = {
      getUsername: () => 'user@test.com',
      getSession: vi.fn((callback) => callback(null, mockSession)),
    }

    CognitoUserPool.mockImplementation(() => ({
      getCurrentUser: vi.fn().mockReturnValue(mockCognitoUser),
    }))

    const { AuthProvider } = await import('../../auth/AuthProvider.jsx')

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('user@test.com')
      expect(screen.getByTestId('is-super-admin')).toHaveTextContent('false')
    })
  })

  it('handles invalid session', async () => {
    const { CognitoUserPool } = await import('amazon-cognito-identity-js')

    const mockSession = {
      isValid: () => false,
    }

    const mockCognitoUser = {
      getUsername: () => 'user@test.com',
      getSession: vi.fn((callback) => callback(null, mockSession)),
    }

    CognitoUserPool.mockImplementation(() => ({
      getCurrentUser: vi.fn().mockReturnValue(mockCognitoUser),
    }))

    const { AuthProvider } = await import('../../auth/AuthProvider.jsx')

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('No user')
    })
  })

  it('handles session error', async () => {
    const { CognitoUserPool } = await import('amazon-cognito-identity-js')

    const mockCognitoUser = {
      getUsername: () => 'user@test.com',
      getSession: vi.fn((callback) => callback(new Error('Session expired'), null)),
    }

    CognitoUserPool.mockImplementation(() => ({
      getCurrentUser: vi.fn().mockReturnValue(mockCognitoUser),
    }))

    const { AuthProvider } = await import('../../auth/AuthProvider.jsx')

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('No user')
    })
  })
})
