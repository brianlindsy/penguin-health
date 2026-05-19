import { useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { RoleGuard } from '../auth/RoleGuard.jsx'
import { usePermissions } from '../auth/usePermissions.js'
import { NLAnalyticsExplorer } from '../components/NLAnalyticsExplorer.jsx'
import { SavedReportsTab } from '../components/SavedReportsTab.jsx'
import { StaffPerformancePage } from './StaffPerformancePage.jsx'
import { RevenueAnalysisPage } from './RevenueAnalysisPage.jsx'

// Tab visibility is computed per-user so non-super-admins still land on the
// hub and only see the existing analytics tabs they have permission for.
function useVisibleTabs() {
  const perms = usePermissions()
  return [
    perms.isSuperAdmin && { key: 'nl-explorer', label: 'NL Explorer' },
    { key: 'reports', label: 'Saved Reports' },
    perms.canViewAnalytics('staff_performance') && {
      key: 'staff-performance', label: 'Staff Performance',
    },
    perms.canViewAnalytics('revenue_analysis') && {
      key: 'revenue-analysis', label: 'Revenue Analysis',
    },
  ].filter(Boolean)
}

export function AnalyticsHubPage() {
  const { orgId } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()
  const tabs = useVisibleTabs()
  const tabKeys = new Set(tabs.map(t => t.key))
  const tabParam = searchParams.get('tab')
  const activeTab = tabKeys.has(tabParam) ? tabParam : tabs[0]?.key

  // Bumps when the user saves a new report, so the Reports tab refetches
  // next time it mounts.
  const [reportsRefreshKey, setReportsRefreshKey] = useState(0)

  function setActiveTab(key) {
    const next = new URLSearchParams(searchParams)
    next.set('tab', key)
    setSearchParams(next, { replace: true })
  }

  if (tabs.length === 0) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-6">
        <h1 className="text-2xl font-semibold text-gray-900 mb-2">Analytics</h1>
        <div className="bg-white shadow rounded-lg p-6 text-sm text-gray-600">
          You don&apos;t have access to any analytics views for this organization.
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-6">
      <h1 className="text-2xl font-semibold text-gray-900 mb-1">Analytics</h1>
      <p className="text-sm text-gray-500 mb-6">
        Ask questions of your data in natural language, or use the prebuilt
        analytics pages.
      </p>

      <div className="border-b border-gray-200 mb-6">
        <nav className="flex gap-8">
          {tabs.map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`py-3 text-sm font-medium border-b-2 ${
                activeTab === tab.key
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      {activeTab === 'nl-explorer' && (
        <RoleGuard requireSuperAdmin>
          <NLAnalyticsExplorer
            orgId={orgId}
            onReportSaved={() => setReportsRefreshKey(k => k + 1)}
          />
        </RoleGuard>
      )}
      {activeTab === 'reports' && (
        <SavedReportsTab orgId={orgId} refreshKey={reportsRefreshKey} />
      )}
      {activeTab === 'staff-performance' && (
        <RoleGuard requireAnalytics="staff_performance">
          <StaffPerformancePage />
        </RoleGuard>
      )}
      {activeTab === 'revenue-analysis' && (
        <RoleGuard requireAnalytics="revenue_analysis">
          <RevenueAnalysisPage />
        </RoleGuard>
      )}
    </div>
  )
}
