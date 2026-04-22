import { useEffect, useState } from 'react'
import { Link, useLocation, useParams } from 'react-router-dom'
import { api } from '../api/client.js'

// Shared left-nav "bumper" for org-scoped workspace pages. Sections + items
// mirror the product mockup (Dashboard, Audit Rules, Analytics & Insights,
// Staff Performance, Revenue Analysis, Compliance Tracker). We also add a
// Validation Results item so users can hop back from Staff Performance.
// Items whose target pages don't exist yet render as disabled.
export function OrgWorkspaceLayout({ children }) {
  const { orgId } = useParams()
  const location = useLocation()
  const pathname = location.pathname
  const tab = new URLSearchParams(location.search).get('tab')

  // Fetch the most recent validation run so the "Today's Validation" shortcut
  // can deep-link straight to its detail page. Runs are ordered by timestamp
  // (desc); if the list is empty the shortcut renders as disabled.
  const [latestRunId, setLatestRunId] = useState(null)

  useEffect(() => {
    if (!orgId) return
    let cancelled = false
    api.listValidationRuns(orgId)
      .then(data => {
        if (cancelled) return
        const runs = (Array.isArray(data) ? data : data?.runs) || []
        if (runs.length === 0) return
        const sorted = [...runs].sort((a, b) => {
          const at = a.timestamp ? new Date(a.timestamp).getTime() : 0
          const bt = b.timestamp ? new Date(b.timestamp).getTime() : 0
          return bt - at
        })
        setLatestRunId(sorted[0].validation_run_id)
      })
      .catch(() => { /* silent — nav just stays disabled */ })
    return () => { cancelled = true }
  }, [orgId])

  const latestRunPath = latestRunId
    ? `/organizations/${orgId}/validation-runs/${latestRunId}`
    : null
  const onLatestRun = !!latestRunPath && pathname === latestRunPath

  const sections = [
    {
      label: 'Overview',
      items: [
        {
          key: 'dashboard',
          label: 'Dashboard',
          to: `/organizations/${orgId}`,
          icon: GridIcon,
          active:
            pathname === `/organizations/${orgId}` &&
            tab !== 'rules' &&
            tab !== 'validation' &&
            tab !== 'field-mappings',
        },
      ],
    },
    {
      label: 'Data Management',
      items: [
        {
          key: 'audit-rules',
          label: 'Audit Rules',
          to: `/organizations/${orgId}/audit-rules`,
          icon: ShieldIcon,
          active:
            pathname.startsWith(`/organizations/${orgId}/audit-rules`) ||
            (pathname === `/organizations/${orgId}` && tab === 'rules'),
        },
        {
          key: 'validation-results',
          label: 'Validation Results',
          to: `/organizations/${orgId}?tab=validation`,
          icon: DocumentIcon,
          // Avoid double-highlighting when we're already on the latest run —
          // that case lights up "Today's Validation" instead.
          active:
            !onLatestRun && (
              pathname.startsWith(`/organizations/${orgId}/validation-runs`) ||
              (pathname === `/organizations/${orgId}` && tab === 'validation')
            ),
        },
        {
          key: 'staff-performance',
          label: 'Staff Performance',
          to: `/organizations/${orgId}/staff-performance`,
          icon: UsersIcon,
          active: pathname === `/organizations/${orgId}/staff-performance`,
        },
        {
          key: 'todays-validation',
          label: "Today's Validation",
          to: latestRunPath,
          icon: BoltIcon,
          active: onLatestRun,
          disabled: !latestRunPath,
          disabledTitle: 'No validation runs yet',
        },
      ],
    },
  ]

  return (
    <div className="flex gap-6">
      <aside className="w-56 flex-shrink-0 self-start sticky top-4 bg-white rounded-lg shadow-sm border border-gray-100 max-h-[calc(100vh-100px)] overflow-y-auto">
        <div className="p-4 border-b border-gray-100 flex items-center gap-3">
          <div className="w-9 h-9 bg-blue-600 rounded-lg flex items-center justify-center flex-shrink-0">
            <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <div className="min-w-0">
            <div className="font-bold text-gray-900 truncate">Penguin Health</div>
            <div className="text-[10px] font-semibold text-blue-700 uppercase tracking-wide">Compliance OS</div>
          </div>
        </div>

        <nav className="p-3 space-y-4">
          {sections.map(section => (
            <div key={section.label}>
              <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider px-2 mb-1.5">
                {section.label}
              </div>
              <div className="space-y-0.5">
                {section.items.map(item => <NavItem key={item.key} item={item} />)}
              </div>
            </div>
          ))}
        </nav>
      </aside>

      <div className="flex-1 min-w-0">
        {children}
      </div>
    </div>
  )
}

function NavItem({ item }) {
  const Icon = item.icon
  const base = 'flex items-center gap-2.5 px-2 py-2 rounded-md text-sm'
  if (item.disabled) {
    return (
      <div
        className={`${base} text-gray-400 cursor-not-allowed`}
        title={item.disabledTitle || 'Coming soon'}
      >
        <Icon className="w-4 h-4 flex-shrink-0" />
        <span className="truncate">{item.label}</span>
      </div>
    )
  }
  const active = item.active
  return (
    <Link
      to={item.to}
      className={`${base} transition-colors ${
        active
          ? 'bg-blue-50 text-blue-700 font-medium ring-1 ring-blue-200'
          : 'text-gray-700 hover:bg-gray-50'
      }`}
    >
      <Icon className={`w-4 h-4 flex-shrink-0 ${active ? 'text-blue-700' : 'text-gray-500'}`} />
      <span className="truncate">{item.label}</span>
    </Link>
  )
}

function GridIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
    </svg>
  )
}

function ShieldIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
    </svg>
  )
}

function UsersIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
    </svg>
  )
}

function DocumentIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
    </svg>
  )
}

function BoltIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
    </svg>
  )
}
