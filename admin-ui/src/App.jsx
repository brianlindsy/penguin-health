import { useEffect } from 'react'
import { Routes, Route, Navigate, useParams } from 'react-router-dom'
import { useAuth } from './auth/useAuth.js'
import { ProtectedRoute } from './auth/ProtectedRoute.jsx'
import { LoginPage } from './auth/LoginPage.jsx'
import { Layout } from './components/Layout.jsx'
import { OrganizationsPage } from './pages/OrganizationsPage.jsx'
import { OrganizationDetail } from './pages/OrganizationDetail.jsx'
import { RuleEditor } from './pages/RuleEditor.jsx'
import { RuleCreator } from './pages/RuleCreator.jsx'
import { DocumentQueuePage } from './pages/DocumentQueuePage.jsx'
import { AuditRulesPage } from './pages/AuditRulesPage.jsx'
import { AuditRuleDetailPage } from './pages/AuditRuleDetailPage.jsx'
import { AnalyticsHubPage } from './pages/AnalyticsHubPage.jsx'
import { SavedReportPage } from './pages/SavedReportPage.jsx'
import { UsersPage } from './pages/UsersPage.jsx'
import { EligibilityPage } from './pages/EligibilityPage.jsx'
import { EligibilityWorklistPage } from './pages/EligibilityWorklistPage.jsx'
import { NotificationPreferencesPage } from './pages/NotificationPreferencesPage.jsx'
import { setTokenProvider, setOnUnauthorized } from './api/client.js'
import { RoleGuard } from './auth/RoleGuard.jsx'

// The old /validation-runs list, /validation-results page, and /dashboard
// all now redirect to the document queue — that's the one reviewer surface.
function DocumentQueueRedirect() {
  const { orgId } = useParams()
  return <Navigate to={`/organizations/${orgId}/document-queue`} replace />
}

// Root landing: users whose Cognito profile carries a `custom:organization_id`
// go straight to their queue. Super-admins / unassigned users keep landing
// on the org picker so they can pick one.
function RootLanding({ user }) {
  if (user?.organizationId) {
    return <Navigate to={`/organizations/${user.organizationId}/document-queue`} replace />
  }
  return <OrganizationsPage />
}

// Legacy per-run detail URL is preserved for bookmarks. Reviewers no longer
// work by-run — the queue is the primary surface — so we drop the runId and
// land on the queue. The `firstSeenRunId` query param carries the original
// context so operators can still narrow to "everything from run X".
function LegacyRunDetailRedirect() {
  const { orgId, runId } = useParams()
  return (
    <Navigate
      to={`/organizations/${orgId}/document-queue?firstSeenRunId=${runId}`}
      replace
    />
  )
}

// Old analytics URLs now live as tabs inside the Analytics hub. Preserve
// bookmarks by redirecting to the hub with the right ?tab=.
function AnalyticsTabRedirect({ tab }) {
  const { orgId } = useParams()
  return (
    <Navigate
      to={`/organizations/${orgId}/analytics?tab=${tab}`}
      replace
    />
  )
}

function App() {
  const { user, loading, getToken, logout } = useAuth()

  useEffect(() => {
    setTokenProvider(getToken)
    setOnUnauthorized(logout)
  }, [getToken, logout])

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <p className="text-gray-500">Loading...</p>
      </div>
    )
  }

  return (
    <Routes>
      <Route path="/login" element={user ? <Navigate to="/" /> : <LoginPage />} />
      <Route element={<ProtectedRoute><Layout /></ProtectedRoute>}>
        {/* Users tied to a specific org land on their queue by default;
            super-admins without an assigned org still hit the picker. */}
        <Route path="/" element={<RootLanding user={user} />} />
        <Route path="/organizations/:orgId" element={<OrganizationDetail />} />
        <Route
          path="/organizations/:orgId/rules/new"
          element={
            <RoleGuard requireOrgAdmin>
              <RuleCreator />
            </RoleGuard>
          }
        />
        <Route
          path="/organizations/:orgId/rules/:ruleId"
          element={
            <RoleGuard requireOrgAdmin>
              <RuleEditor />
            </RoleGuard>
          }
        />
        <Route path="/organizations/:orgId/validation-runs" element={<DocumentQueueRedirect />} />
        <Route path="/organizations/:orgId/validation-runs/:runId" element={<LegacyRunDetailRedirect />} />
        <Route path="/organizations/:orgId/document-queue" element={<DocumentQueuePage />} />
        <Route
          path="/organizations/:orgId/analytics"
          element={<AnalyticsHubPage />}
        />
        <Route
          path="/organizations/:orgId/analytics/reports/:reportId"
          element={<SavedReportPage />}
        />
        <Route
          path="/organizations/:orgId/staff-performance"
          element={<AnalyticsTabRedirect tab="staff-performance" />}
        />
        <Route path="/organizations/:orgId/audit-rules" element={<AuditRulesPage />} />
        <Route path="/organizations/:orgId/audit-rules/:ruleId" element={<AuditRuleDetailPage />} />
        {/* Legacy URLs — preserve bookmarks; queue is the one reviewer surface. */}
        <Route path="/organizations/:orgId/validation-results" element={<DocumentQueueRedirect />} />
        <Route
          path="/organizations/:orgId/revenue-analysis"
          element={<AnalyticsTabRedirect tab="revenue-analysis" />}
        />
        <Route path="/organizations/:orgId/dashboard" element={<DocumentQueueRedirect />} />
        <Route path="/organizations/:orgId/eligibility" element={<EligibilityPage />} />
        <Route path="/organizations/:orgId/eligibility/worklist" element={<EligibilityWorklistPage />} />
        {/* Legacy URL — preserve bookmarks. */}
        <Route
          path="/organizations/:orgId/eligibility/census"
          element={<Navigate to="../eligibility/worklist" replace relative="path" />}
        />
        <Route
          path="/organizations/:orgId/users"
          element={
            <RoleGuard requireSuperAdmin>
              <UsersPage />
            </RoleGuard>
          }
        />
        <Route
          path="/organizations/:orgId/settings/notifications"
          element={
            <RoleGuard requireSuperAdmin>
              <NotificationPreferencesPage />
            </RoleGuard>
          }
        />
      </Route>
    </Routes>
  )
}

export default App
