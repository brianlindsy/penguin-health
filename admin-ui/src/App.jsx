import { useEffect } from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import { useAuth } from './auth/AuthProvider.jsx'
import { ProtectedRoute } from './auth/ProtectedRoute.jsx'
import { LoginPage } from './auth/LoginPage.jsx'
import { Layout } from './components/Layout.jsx'
import { OrganizationsPage } from './pages/OrganizationsPage.jsx'
import { OrganizationDetail } from './pages/OrganizationDetail.jsx'
import { RuleEditor } from './pages/RuleEditor.jsx'
import { setTokenProvider, setOnUnauthorized } from './api/client.js'

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
        <Route path="/organizations/:orgId/rules/:ruleId" element={<RuleEditor />} />
        <Route path="/organizations/:orgId/rules/new" element={<RuleEditor />} />
      </Route>
    </Routes>
  )
}

export default App
