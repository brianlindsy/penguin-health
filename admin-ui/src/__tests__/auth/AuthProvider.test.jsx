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

// Use vi.hoisted to create mocks that are available when vi.mock is hoisted
const { mockGetCurrentUser } = vi.hoisted(() => {
  return {
    mockGetCurrentUser: vi.fn(),
  }
})

vi.mock('amazon-cognito-identity-js', () => ({
  CognitoUserPool: vi.fn().mockImplementation(() => ({
    getCurrentUser: mockGetCurrentUser,
  })),
  CognitoUser: vi.fn(),
  AuthenticationDetails: vi.fn(),
}))

// Import after mocking
import { AuthProvider, useAuth } from '../../auth/AuthProvider.jsx'

// Test component to access auth context
function TestConsumer() {
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
    mockGetCurrentUser.mockReturnValue(null)
  })

  it('shows no user when not logged in', async () => {
    mockGetCurrentUser.mockReturnValue(null)

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
      getSession: (callback) => callback(null, mockSession),
    }

    mockGetCurrentUser.mockReturnValue(mockCognitoUser)

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('admin@test.com')
    })

    expect(screen.getByTestId('is-super-admin')).toHaveTextContent('true')
    expect(screen.getByTestId('org-id')).toHaveTextContent('org-123')
  })

  it('detects non-super-admin users', async () => {
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
      getSession: (callback) => callback(null, mockSession),
    }

    mockGetCurrentUser.mockReturnValue(mockCognitoUser)

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('user@test.com')
    })

    expect(screen.getByTestId('is-super-admin')).toHaveTextContent('false')
  })

  it('handles invalid session', async () => {
    const mockSession = {
      isValid: () => false,
    }

    const mockCognitoUser = {
      getUsername: () => 'user@test.com',
      getSession: (callback) => callback(null, mockSession),
    }

    mockGetCurrentUser.mockReturnValue(mockCognitoUser)

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
    const mockCognitoUser = {
      getUsername: () => 'user@test.com',
      getSession: (callback) => callback(new Error('Session expired'), null),
    }

    mockGetCurrentUser.mockReturnValue(mockCognitoUser)

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('No user')
    })
  })

  it('handles missing cognito groups gracefully', async () => {
    const mockIdToken = {
      getJwtToken: () => 'mock-jwt-token',
      payload: {
        email: 'user@test.com',
        // No cognito:groups field
      },
    }

    const mockSession = {
      isValid: () => true,
      getIdToken: () => mockIdToken,
    }

    const mockCognitoUser = {
      getUsername: () => 'user@test.com',
      getSession: (callback) => callback(null, mockSession),
    }

    mockGetCurrentUser.mockReturnValue(mockCognitoUser)

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    )

    await waitFor(() => {
      expect(screen.getByTestId('user')).toHaveTextContent('user@test.com')
    })

    expect(screen.getByTestId('is-super-admin')).toHaveTextContent('false')
  })
})
