import { useState } from 'react'
import { useAuth } from './AuthProvider.jsx'

export function LoginPage() {
  const { login, completeNewPassword } = useAuth()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [challengeData, setChallengeData] = useState(null)

  const handleLogin = async (e) => {
    e.preventDefault()
    setError('')
    setSubmitting(true)

    try {
      const result = await login(email, password)
      if (result?.newPasswordRequired) {
        setChallengeData(result)
      }
    } catch (err) {
      setError(err.message || 'Login failed')
    } finally {
      setSubmitting(false)
    }
  }

  const handleNewPassword = async (e) => {
    e.preventDefault()
    setError('')
    setSubmitting(true)

    try {
      await completeNewPassword(challengeData.cognitoUser, newPassword)
    } catch (err) {
      setError(err.message || 'Password change failed')
    } finally {
      setSubmitting(false)
    }
  }

  if (challengeData) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="w-full max-w-sm p-8 bg-white rounded-lg shadow">
          <h1 className="text-xl font-semibold text-gray-900 mb-2">Set New Password</h1>
          <p className="text-sm text-gray-600 mb-6">You must set a new password on first login.</p>

          {error && (
            <div className="mb-4 p-3 bg-red-50 text-red-700 text-sm rounded">{error}</div>
          )}

          <form onSubmit={handleNewPassword}>
            <label className="block text-sm font-medium text-gray-700 mb-1">New Password</label>
            <input
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md mb-4 focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
              minLength={12}
            />
            <button
              type="submit"
              disabled={submitting}
              className="w-full py-2 px-4 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              {submitting ? 'Setting...' : 'Set Password'}
            </button>
          </form>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="w-full max-w-sm p-8 bg-white rounded-lg shadow">
        <h1 className="text-xl font-semibold text-gray-900 mb-1">Penguin Health</h1>
        <p className="text-sm text-gray-600 mb-6">Admin Console</p>

        {error && (
          <div className="mb-4 p-3 bg-red-50 text-red-700 text-sm rounded">{error}</div>
        )}

        <form onSubmit={handleLogin}>
          <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md mb-4 focus:outline-none focus:ring-2 focus:ring-blue-500"
            required
          />

          <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md mb-6 focus:outline-none focus:ring-2 focus:ring-blue-500"
            required
          />

          <button
            type="submit"
            disabled={submitting}
            className="w-full py-2 px-4 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {submitting ? 'Signing in...' : 'Sign In'}
          </button>
        </form>
      </div>
    </div>
  )
}
