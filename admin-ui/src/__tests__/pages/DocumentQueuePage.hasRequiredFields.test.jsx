import { describe, it, expect } from 'vitest'
import { hasRequiredFields } from '../../pages/DocumentQueuePage.jsx'

// This test pins the render-gate behavior directly on the exported
// helper without pulling the whole page into a render(). The gate
// decides whether a document appears in the run's document list; a
// regression here makes docs silently disappear from the UI.

describe('hasRequiredFields', () => {
  describe('supportive-care (ungated)', () => {
    it('always passes for supportive-care regardless of field_values shape', () => {
      // centralreach docs land under supportive-care but don't carry
      // diagnosis_code / employee_name — those are Credible-BH CSV
      // fields. Without the org bypass, every one gets hidden.
      const doc = { field_values: { source_record_id: '1234' } }
      expect(hasRequiredFields(doc, 'supportive-care')).toBe(true)
    })

    it('passes for supportive-care even with completely empty field_values', () => {
      expect(hasRequiredFields({ field_values: {} }, 'supportive-care')).toBe(true)
    })

    it('passes for supportive-care even without field_values at all', () => {
      expect(hasRequiredFields({}, 'supportive-care')).toBe(true)
    })
  })

  describe('other orgs — legacy diagnosis_code + employee_name gate', () => {
    it('requires both diagnosis_code and employee_name by default', () => {
      const doc = {
        field_values: {
          diagnosis_code: 'F33.1',
          employee_name: 'Jane Doe',
        },
      }
      expect(hasRequiredFields(doc, 'other-org')).toBe(true)
    })

    it('fails when diagnosis_code is missing', () => {
      const doc = { field_values: { employee_name: 'Jane Doe' } }
      expect(hasRequiredFields(doc, 'other-org')).toBe(false)
    })

    it('fails when employee_name is missing', () => {
      const doc = { field_values: { diagnosis_code: 'F33.1' } }
      expect(hasRequiredFields(doc, 'other-org')).toBe(false)
    })

    it('fails when field_values is missing entirely', () => {
      expect(hasRequiredFields({}, 'other-org')).toBe(false)
    })

    it('passes for BedDay-Psych service type regardless of missing diagnosis/employee', () => {
      const doc = { field_values: { service_type: 'BedDay-Psych' } }
      expect(hasRequiredFields(doc, 'other-org')).toBe(true)
    })

    it('passes for BedDay-Detox service type regardless of missing diagnosis/employee', () => {
      const doc = { field_values: { service_type: 'BedDay-Detox' } }
      expect(hasRequiredFields(doc, 'other-org')).toBe(true)
    })
  })
})
