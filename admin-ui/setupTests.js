/**
 * Vitest setup file for React component testing.
 *
 * Configures:
 * - jest-dom matchers
 * - Environment variable mocks
 * - Browser API mocks (matchMedia, etc.)
 * - Cognito SDK mocks
 */

import '@testing-library/jest-dom'
import { cleanup } from '@testing-library/react'
import { afterEach, beforeAll, afterAll, vi } from 'vitest'
import { server } from './src/__tests__/mocks/server.js'
import { resetUserPermStore } from './src/__tests__/mocks/handlers.js'

// Bring up MSW for the duration of the test session so any request the
// frontend fires (e.g. /api/me/permissions on AuthProvider mount) hits a
// mock instead of the network. Tests can override handlers via server.use().
beforeAll(() => server.listen({ onUnhandledRequest: 'bypass' }))
afterEach(() => {
  cleanup()
  server.resetHandlers()
  resetUserPermStore()
})
afterAll(() => server.close())

// Mock environment variables
vi.stubEnv('VITE_API_URL', 'http://localhost:3000')
vi.stubEnv('VITE_COGNITO_USER_POOL_ID', 'us-east-1_testpool')
vi.stubEnv('VITE_COGNITO_CLIENT_ID', 'testclientid')

// Mock window.matchMedia for responsive components
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation(query => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
})

// Mock IntersectionObserver
class MockIntersectionObserver {
  constructor() {
    this.observe = vi.fn()
    this.unobserve = vi.fn()
    this.disconnect = vi.fn()
  }
}

Object.defineProperty(window, 'IntersectionObserver', {
  writable: true,
  value: MockIntersectionObserver,
})

// Mock ResizeObserver
class MockResizeObserver {
  constructor() {
    this.observe = vi.fn()
    this.unobserve = vi.fn()
    this.disconnect = vi.fn()
  }
}

Object.defineProperty(window, 'ResizeObserver', {
  writable: true,
  value: MockResizeObserver,
})

// Mock scrollTo
window.scrollTo = vi.fn()

// Suppress console.error for expected test warnings
const originalError = console.error
console.error = (...args) => {
  // Suppress React act() warnings in tests
  if (args[0]?.includes?.('Warning: An update to') ||
      args[0]?.includes?.('Warning: ReactDOM.render')) {
    return
  }
  originalError.call(console, ...args)
}
