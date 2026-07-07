import { describe, it, expect } from 'vitest'
import { getDocumentLink } from '../../pages/ValidationRunDetailPage.jsx'

describe('getDocumentLink', () => {
  describe('CentralReach orgs', () => {
    it('links supportive-care docs to the CentralReach resource-details deep link by preview_file_id', () => {
      const doc = {
        document_id: 'abc-123',
        field_values: { service_id: 'svc-999', preview_file_id: 8901 },
      }
      expect(getDocumentLink('supportive-care', doc)).toBe(
        'https://members.centralreach.com/#resources/details?id=8901'
      )
    })

    it('ignores service_id for CentralReach orgs (preview_file_id drives the URL)', () => {
      const doc = {
        document_id: 'abc-123',
        field_values: { service_id: 'svc-999', preview_file_id: 8901 },
      }
      expect(getDocumentLink('supportive-care', doc)).not.toContain('svc-999')
    })

    it('falls back to document_id when preview_file_id is missing (legacy records)', () => {
      const doc = { document_id: 'abc-123', field_values: { service_id: 'svc-999' } }
      expect(getDocumentLink('supportive-care', doc)).toBe(
        'https://members.centralreach.com/#resources/details?id=abc-123'
      )
    })
  })

  describe('non-CentralReach orgs (Credible BH fallback)', () => {
    it('prefers service_id over document_id when present', () => {
      const doc = { document_id: 'doc-1', field_values: { service_id: 'svc-42' } }
      expect(getDocumentLink('other-org', doc)).toBe(
        'https://www.cbh3.crediblebh.com/visit/clientvisit_view.asp?clientvisit_id=svc-42&provportal=0'
      )
    })

    it('falls back to document_id when service_id is missing', () => {
      const doc = { document_id: 'doc-1', field_values: {} }
      expect(getDocumentLink('other-org', doc)).toBe(
        'https://www.cbh3.crediblebh.com/visit/clientvisit_view.asp?clientvisit_id=doc-1&provportal=0'
      )
    })
  })
})
