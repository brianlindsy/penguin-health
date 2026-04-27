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
import { afterEach, vi } from 'vitest'

// Cleanup after each test
afterEach(() => {
  cleanup()
})

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
