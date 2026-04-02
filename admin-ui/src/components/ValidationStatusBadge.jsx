export function ValidationStatusBadge({ status }) {
  const styles = {
    PASS: 'bg-green-100 text-green-800',
    FAIL: 'bg-red-100 text-red-800',
    SKIP: 'bg-yellow-100 text-yellow-800',
    ERROR: 'bg-gray-100 text-gray-600',
  }

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${styles[status] || styles.ERROR}`}
    >
      {status}
    </span>
  )
}
