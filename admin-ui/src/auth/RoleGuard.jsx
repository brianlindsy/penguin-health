import { usePermissions } from './usePermissions.js'

// Declarative permission gate for routes and elements.
// Renders children when every supplied requirement is met; otherwise renders
// the fallback (default: a 403 message). This is purely a UX gate — the
// backend independently enforces permissions on every API call.
//
// Props (any combination):
//   requireSuperAdmin       boolean
//   requireOrgAdmin         boolean
//   requireViewCategory     string  (e.g. 'Billing')
//   requireRunCategory      string
//   requireAnalytics        string  ('staff_performance' | 'revenue_analysis')
//   fallback                ReactNode  shown when denied
export function RoleGuard({
  children,
  requireSuperAdmin = false,
  requireOrgAdmin = false,
  requireViewCategory,
  requireRunCategory,
  requireAnalytics,
  fallback = <Forbidden />,
}) {
  const perms = usePermissions()

  // Avoid flashing the fallback before permissions arrive. Super-admins are
  // known immediately from the JWT; everyone else waits for /api/me/permissions.
  if (!perms.ready) return null

  if (requireSuperAdmin && !perms.isSuperAdmin) return fallback
  if (requireOrgAdmin && !perms.isOrgAdmin) return fallback
  if (requireViewCategory && !perms.canViewCategory(requireViewCategory)) return fallback
  if (requireRunCategory && !perms.canRunCategory(requireRunCategory)) return fallback
  if (requireAnalytics && !perms.canViewAnalytics(requireAnalytics)) return fallback

  return children
}

function Forbidden() {
  return (
    <div className="flex items-center justify-center py-16">
      <div className="text-center">
        <h2 className="text-lg font-semibold text-gray-900">Access denied</h2>
        <p className="text-sm text-gray-500 mt-1">
          You don't have permission to view this page.
        </p>
      </div>
    </div>
  )
}
