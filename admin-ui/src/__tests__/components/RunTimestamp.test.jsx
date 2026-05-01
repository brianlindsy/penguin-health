import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { RunTimestamp } from '../../components/RunTimestamp.jsx'

describe('RunTimestamp', () => {
  it('shows fallback when value is missing', () => {
    render(<RunTimestamp value={null} fallback="(none)" />)
    expect(screen.getByText('(none)')).toBeInTheDocument()
  })

  it('renders date + time + timezone abbreviation', () => {
    // Pick an arbitrary fixed UTC instant.
    render(<RunTimestamp value="2026-05-15T18:42:00Z" />)
    // The exact local string varies by test runner TZ. Two assertions that
    // are robust regardless: the year is shown, and the timeZoneName=short
    // option produces a non-empty TZ token at the end (letters).
    const node = screen.getByText(/2026/)
    expect(node).toBeInTheDocument()
    expect(node.textContent).toMatch(/[A-Z]{2,5}$/)
  })
})
