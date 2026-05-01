import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { RunDates } from '../../components/RunDates.jsx'

describe('RunDates', () => {
  it('renders em-dash when dates is undefined (legacy run)', () => {
    render(<RunDates />)
    expect(screen.getByText('—')).toBeInTheDocument()
  })

  it('renders em-dash when dates is empty', () => {
    render(<RunDates dates={[]} />)
    expect(screen.getByText('—')).toBeInTheDocument()
  })

  it('renders a single date', () => {
    render(<RunDates dates={['2026-05-15']} />)
    expect(screen.getByText(/May 15, 2026/)).toBeInTheDocument()
  })

  it('collapses a contiguous range to "Month start – end, year"', () => {
    render(<RunDates dates={['2026-05-01', '2026-05-02', '2026-05-03']} />)
    // Same month + year — expect a single hyphen-separated label.
    expect(screen.getByText(/May 1\s*–\s*3,\s*2026/)).toBeInTheDocument()
  })

  it('renders comma list for non-contiguous dates', () => {
    render(<RunDates dates={['2026-05-01', '2026-05-03']} />)
    expect(screen.getByText(/May 1, 2026, May 3, 2026/)).toBeInTheDocument()
  })

  it('sorts unsorted input before deciding contiguity', () => {
    render(<RunDates dates={['2026-05-03', '2026-05-01', '2026-05-02']} />)
    expect(screen.getByText(/May 1\s*–\s*3,\s*2026/)).toBeInTheDocument()
  })
})
