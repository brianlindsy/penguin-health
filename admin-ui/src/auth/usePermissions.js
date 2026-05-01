import { useMemo } from 'react'
import { useAuth } from './AuthProvider.jsx'

export const CATEGORIES = ['Intake', 'Billing', 'Compliance Audit', 'Quality Assurance']
export const ANALYTICS_PAGES = ['staff_performance', 'revenue_analysis']

// Lightweight wrapper around the permissions object the backend hands us at
// /api/me/permissions. The frontend only uses these helpers to drive
// conditional rendering — the real authorization runs server-side on every
// API call.
export function usePermissions() {
  const { permissions, isSuperAdmin } = useAuth()

  return useMemo(() => {
    const isOrgAdmin = isSuperAdmin || permissions?.role === 'org_admin'
    const reportPerms = permissions?.report_permissions || {}
    const analyticsPerms = permissions?.analytics_permissions || []

    const canViewCategory = (category) => {
      if (isOrgAdmin) return true
      const verbs = reportPerms[category]
      return Array.isArray(verbs) && verbs.includes('view')
    }

    const canRunCategory = (category) => {
      if (isOrgAdmin) return true
      const verbs = reportPerms[category]
      return Array.isArray(verbs) && verbs.includes('run')
    }

    const viewableCategories = () => {
      if (isOrgAdmin) return new Set(CATEGORIES)
      return new Set(
        Object.entries(reportPerms)
          .filter(([_, verbs]) => Array.isArray(verbs) && verbs.includes('view'))
          .map(([cat]) => cat)
      )
    }

    const runnableCategories = () => {
      if (isOrgAdmin) return new Set(CATEGORIES)
      return new Set(
        Object.entries(reportPerms)
          .filter(([_, verbs]) => Array.isArray(verbs) && verbs.includes('run'))
          .map(([cat]) => cat)
      )
    }

    const canViewAnalytics = (page) => {
      if (isOrgAdmin) return true
      return analyticsPerms.includes(page)
    }

    return {
      // Loading state — null means we haven't received permissions yet (or the
      // request failed). UI should treat this as "not yet known" and avoid
      // making access decisions until the value is set.
      ready: permissions !== null || isSuperAdmin,
      isSuperAdmin,
      isOrgAdmin,
      canViewCategory,
      canRunCategory,
      viewableCategories,
      runnableCategories,
      canViewAnalytics,
    }
  }, [permissions, isSuperAdmin])
}
