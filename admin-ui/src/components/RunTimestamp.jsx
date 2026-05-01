// Render an ISO-8601 timestamp in the user's local timezone, with the TZ
// abbreviation shown so it's unambiguous (e.g. "May 1, 2026, 3:42 PM EDT").
// Used wherever a validation run's "when did this happen" timestamp is
// displayed, so format stays consistent across the app.
//
// We use explicit format options rather than dateStyle/timeStyle: combining
// the *Style shortcuts with timeZoneName is rejected in some V8 builds
// (notably the one jsdom ships with), and falling back at the call site is
// clumsier than just specifying the parts we want here.
export function RunTimestamp({ value, fallback = '-' }) {
  if (!value) return <>{fallback}</>
  const formatted = new Date(value).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  })
  return <>{formatted}</>
}
