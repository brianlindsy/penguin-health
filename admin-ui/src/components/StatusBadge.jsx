export function StatusBadge({ enabled }) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
        enabled
          ? 'bg-green-100 text-green-800'
          : 'bg-gray-100 text-gray-600'
      }`}
    >
      {enabled ? 'Enabled' : 'Disabled'}
    </span>
  )
}
