import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { StatusBadge } from '../components/StatusBadge.jsx'

export function OrganizationsPage() {
  const [orgs, setOrgs] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    api.listOrganizations()
      .then(data => setOrgs(data.organizations || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <p className="text-gray-500">Loading organizations...</p>
  if (error) return <p className="text-red-600">Error: {error}</p>

  return (
    <div>
      <h1 className="text-2xl font-semibold text-gray-900 mb-6">Organizations</h1>

      <div className="bg-white shadow rounded-lg overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Organization</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">S3 Bucket</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {orgs.map(org => (
              <tr key={org.organization_id} className="hover:bg-gray-50">
                <td className="px-6 py-4">
                  <Link
                    to={`/organizations/${org.organization_id}`}
                    className="text-blue-600 hover:text-blue-800 font-medium"
                  >
                    {org.organization_name}
                  </Link>
                </td>
                <td className="px-6 py-4 text-sm text-gray-600 font-mono">{org.organization_id}</td>
                <td className="px-6 py-4 text-sm text-gray-600 font-mono">{org.s3_bucket_name}</td>
                <td className="px-6 py-4"><StatusBadge enabled={org.enabled} /></td>
              </tr>
            ))}
            {orgs.length === 0 && (
              <tr>
                <td colSpan={4} className="px-6 py-8 text-center text-gray-500">
                  No organizations found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
