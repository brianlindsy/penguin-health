import { Outlet, Link, useLocation } from 'react-router-dom'
import { useAuth } from '../auth/AuthProvider.jsx'
import { useEffect } from 'react'
import { setTokenProvider } from '../api/client.js'

export function Layout() {
  const { user, logout, getToken } = useAuth()
  const location = useLocation()

  useEffect(() => {
    setTokenProvider(getToken)
  }, [getToken])

  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white border-b border-gray-200">
        <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-14 items-center">
            <div className="flex items-center gap-6">
              <Link to="/" className="text-lg font-semibold text-gray-900">
                Penguin Health
              </Link>
              <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded">Admin</span>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-sm text-gray-600">{user?.email}</span>
              <button
                onClick={logout}
                className="text-sm text-gray-500 hover:text-gray-700"
              >
                Sign Out
              </button>
            </div>
          </div>
        </div>
      </nav>

      {/* Breadcrumb */}
      {location.pathname !== '/' && (
        <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-2 flex items-center text-sm">
          <Link to="/" className="text-blue-600 hover:text-blue-800">
            Organizations
          </Link>
          {(() => {
            const segments = location.pathname.split('/').filter(Boolean)
            const breadcrumbs = []

            // Build clickable breadcrumb links for each segment
            for (let i = 1; i < segments.length; i++) {
              const path = '/' + segments.slice(0, i + 1).join('/')
              const isLast = i === segments.length - 1

              breadcrumbs.push(
                <span key={`sep-${i}`} className="text-gray-400 mx-2">/</span>
              )

              if (isLast) {
                breadcrumbs.push(
                  <span key={path} className="text-gray-600">{segments[i]}</span>
                )
              } else {
                breadcrumbs.push(
                  <Link key={path} to={path} className="text-blue-600 hover:text-blue-800">
                    {segments[i]}
                  </Link>
                )
              }
            }

            return breadcrumbs
          })()}
        </div>
      )}

      <main className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <Outlet />
      </main>
    </div>
  )
}
