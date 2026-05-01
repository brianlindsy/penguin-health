// Render the `dates` field of a validation run. Each date is the YYYY-MM-DD
// ingest date of the underlying data the run validated (UTC on the wire,
// converted to the user's locale for display).
//
// Behavior:
//   - empty / missing  -> em-dash (legacy pre-cutover runs)
//   - single date      -> formatted single date
//   - contiguous range -> "May 1 – 3, 2026"
//   - non-contiguous   -> comma list
export function RunDates({ dates }) {
  const list = Array.isArray(dates) ? [...dates].sort() : []

  if (list.length === 0) {
    return <span className="text-xs text-gray-400 italic">—</span>
  }

  if (list.length === 1) {
    return <span className="text-xs text-gray-700">{formatOne(list[0])}</span>
  }

  if (isContiguous(list)) {
    return (
      <span className="text-xs text-gray-700">
        {formatRange(list[0], list[list.length - 1])}
      </span>
    )
  }

  return (
    <span className="text-xs text-gray-700">
      {list.map(formatOne).join(', ')}
    </span>
  )
}

function parseISO(s) {
  // Treat the YYYY-MM-DD as a calendar date in the user's local timezone.
  // Date.UTC + getUTCDate avoids timezone-offset shifts that `new Date('2026-05-01')`
  // would introduce in non-UTC locales.
  const [y, m, d] = s.split('-').map(Number)
  return new Date(y, m - 1, d)
}

function formatOne(s) {
  return parseISO(s).toLocaleDateString(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

function formatRange(startISO, endISO) {
  const start = parseISO(startISO)
  const end = parseISO(endISO)
  const sameYear = start.getFullYear() === end.getFullYear()
  const sameMonth = sameYear && start.getMonth() === end.getMonth()
  if (sameMonth) {
    return `${start.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}` +
      ` – ${end.getDate()}, ${end.getFullYear()}`
  }
  if (sameYear) {
    return `${start.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}` +
      ` – ${end.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}, ${end.getFullYear()}`
  }
  return `${formatOne(startISO)} – ${formatOne(endISO)}`
}

function isContiguous(sortedISOList) {
  for (let i = 1; i < sortedISOList.length; i++) {
    const prev = parseISO(sortedISOList[i - 1])
    const curr = parseISO(sortedISOList[i])
    const diffDays = Math.round((curr - prev) / (24 * 60 * 60 * 1000))
    if (diffDays !== 1) return false
  }
  return true
}
