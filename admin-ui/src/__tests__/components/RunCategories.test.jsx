import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { RunCategories } from '../../components/RunCategories.jsx'

describe('RunCategories', () => {
  it('renders "All" when categories is undefined (legacy run)', () => {
    render(<RunCategories />)
    expect(screen.getByText('All')).toBeInTheDocument()
  })

  it('renders "All" when categories is an empty array', () => {
    render(<RunCategories categories={[]} />)
    expect(screen.getByText('All')).toBeInTheDocument()
  })

  it('renders "All" when every canonical category is present', () => {
    render(<RunCategories categories={[
      'Intake', 'Billing', 'Compliance Audit', 'Quality Assurance',
    ]} />)
    expect(screen.getByText('All')).toBeInTheDocument()
  })

  it('renders chips for a partial set in canonical order', () => {
    // Pass them in arbitrary order; component should reorder.
    render(<RunCategories categories={['Quality Assurance', 'Billing']} />)
    const chips = screen.getAllByText(/Billing|Quality Assurance/)
    expect(chips).toHaveLength(2)
    // Canonical order: Billing before Quality Assurance.
    expect(chips[0]).toHaveTextContent('Billing')
    expect(chips[1]).toHaveTextContent('Quality Assurance')
  })

  it('does not render unknown categories', () => {
    render(<RunCategories categories={['Billing', 'Madeup']} />)
    expect(screen.getByText('Billing')).toBeInTheDocument()
    expect(screen.queryByText('Madeup')).not.toBeInTheDocument()
  })
})
