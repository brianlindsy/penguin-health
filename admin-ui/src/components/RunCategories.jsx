import { CATEGORIES } from '../auth/usePermissions.js'

// Render the categories field of a validation run as either:
//   - "All" (if every canonical category is covered, or if the field is absent
//     — pre-RBAC runs were always run against the full rule set).
//   - A row of small chips with the category names, in canonical order.
export function RunCategories({ categories }) {
  const list = Array.isArray(categories) ? categories : []

  if (list.length === 0) {
    return <span className="text-xs text-gray-500 italic">All</span>
  }

  const set = new Set(list)
  const coversAll = CATEGORIES.every(c => set.has(c))
  if (coversAll) {
    return <span className="text-xs text-gray-500 italic">All</span>
  }

  // Show in canonical order so two runs with the same set always look the same.
  const sorted = CATEGORIES.filter(c => set.has(c))
  return (
    <div className="flex flex-wrap gap-1">
      {sorted.map(c => (
        <span
          key={c}
          className="text-xs bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded"
        >
          {c}
        </span>
      ))}
    </div>
  )
}
