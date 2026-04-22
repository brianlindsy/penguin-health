import { useEffect } from 'react'
import { Routes, Route, Navigate, useParams } from 'react-router-dom'
import { useAuth } from './auth/AuthProvider.jsx'
import { ProtectedRoute } from './auth/ProtectedRoute.jsx'
import { LoginPage } from './auth/LoginPage.jsx'
import { Layout } from './components/Layout.jsx'
import { OrganizationsPage } from './pages/OrganizationsPage.jsx'
import { OrganizationDetail } from './pages/OrganizationDetail.jsx'
import { RuleEditor } from './pages/RuleEditor.jsx'
import { RuleCreator } from './pages/RuleCreator.jsx'
import { ValidationRunDetailPage } from './pages/ValidationRunDetailPage.jsx'
import { StaffPerformancePage } from './pages/StaffPerformancePage.jsx'
import { AuditRulesPage } from './pages/AuditRulesPage.jsx'
import { AuditRuleDetailPage } from './pages/AuditRuleDetailPage.jsx'
import { ValidationResultsPage } from './pages/ValidationResultsPage.jsx'
import { setTokenProvider, setOnUnauthorized } from './api/client.js'

// Redirect /organizations/:orgId/validation-runs to org detail with validation tab
function ValidationRunsRedirect() {
  const { orgId } = useParams()
  return <Navigate to={`/organizations/${orgId}?tab=validation`} replace />
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
        <Route path="/" element={<OrganizationsPage />} />
        <Route path="/organizations/:orgId" element={<OrganizationDetail />} />
        <Route path="/organizations/:orgId/rules/new" element={<RuleCreator />} />
        <Route path="/organizations/:orgId/rules/:ruleId" element={<RuleEditor />} />
        <Route path="/organizations/:orgId/validation-runs" element={<ValidationRunsRedirect />} />
        <Route path="/organizations/:orgId/validation-runs/:runId" element={<ValidationRunDetailPage />} />
        <Route path="/organizations/:orgId/staff-performance" element={<StaffPerformancePage />} />
        <Route path="/organizations/:orgId/audit-rules" element={<AuditRulesPage />} />
        <Route path="/organizations/:orgId/audit-rules/:ruleId" element={<AuditRuleDetailPage />} />
        <Route path="/organizations/:orgId/validation-results" element={<ValidationResultsPage />} />
      </Route>
    </Routes>
  )
}

export default App
