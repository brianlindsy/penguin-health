import { Link, useLocation, useParams } from 'react-router-dom'

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
          to: `/organizations/${orgId}?tab=rules`,
          icon: ShieldIcon,
          active: pathname === `/organizations/${orgId}` && tab === 'rules',
        },
        {
          key: 'analytics',
          label: 'Analytics & Insights',
          icon: BellIcon,
          disabled: true,
        },
        {
          key: 'validation-results',
          label: 'Validation Results',
          to: `/organizations/${orgId}?tab=validation`,
          icon: DocumentIcon,
          active:
            pathname.startsWith(`/organizations/${orgId}/validation-runs`) ||
            (pathname === `/organizations/${orgId}` && tab === 'validation'),
        },
        {
          key: 'staff-performance',
          label: 'Staff Performance',
          to: `/organizations/${orgId}/staff-performance`,
          icon: UsersIcon,
          active: pathname === `/organizations/${orgId}/staff-performance`,
        },
        {
          key: 'revenue',
          label: 'Revenue Analysis',
          icon: DollarIcon,
          disabled: true,
        },
        {
          key: 'compliance-tracker',
          label: 'Compliance Tracker',
          icon: CalendarIcon,
          disabled: true,
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
        title="Coming soon"
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

function BellIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
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

function DollarIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  )
}

function CalendarIcon({ className }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
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
