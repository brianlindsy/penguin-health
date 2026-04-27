/**
 * MSW server setup for Node.js environment (Vitest).
 *
 * Import this in tests that need API mocking:
 *
 *   import { server } from '../mocks/server.js'
 *
 *   beforeAll(() => server.listen())
 *   afterEach(() => server.resetHandlers())
 *   afterAll(() => server.close())
 */

import { setupServer } from 'msw/node'
import { handlers } from './handlers.js'

export const server = setupServer(...handlers)
