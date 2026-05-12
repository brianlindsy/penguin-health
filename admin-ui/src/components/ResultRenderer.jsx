import { useState } from 'react'
import {
  BarChart, Bar,
  LineChart, Line,
  PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, Legend,
  CartesianGrid, ResponsiveContainer,
} from 'recharts'

// Athena returns every value as a string. For numeric columns we coerce so
// charts can plot them; non-numeric or non-coercible values stay as strings.
const NUMERIC_TYPES = new Set([
  'tinyint', 'smallint', 'integer', 'int', 'bigint',
  'float', 'real', 'double', 'decimal', 'numeric',
])

function coerceCell(value, type) {
  if (value === null || value === undefined) return null
  if (!NUMERIC_TYPES.has(String(type || '').toLowerCase())) return value
  const n = Number(value)
  return Number.isFinite(n) ? n : value
}

function rowsToChartData(columns, rows) {
  if (!columns || columns.length === 0) return []
  return rows.map(row => {
    const obj = {}
    columns.forEach((col, i) => {
      obj[col.name] = coerceCell(row[i], col.type)
    })
    return obj
  })
}

const PIE_COLORS = [
  '#3b82f6', '#10b981', '#f59e0b', '#ef4444',
  '#8b5cf6', '#06b6d4', '#ec4899', '#84cc16',
]

function TableView({ columns, rows }) {
  if (!rows || rows.length === 0) {
    return <p className="text-sm text-gray-500 italic p-4">No rows returned.</p>
  }
  return (
    <div className="bg-white shadow rounded-lg overflow-auto max-h-[600px]">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50 sticky top-0">
          <tr>
            {columns.map(col => (
              <th
                key={col.name}
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase whitespace-nowrap"
              >
                {col.name}
                <span className="ml-1 text-gray-400 font-normal normal-case">
                  ({col.type})
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200">
          {rows.map((row, i) => (
            <tr key={i} className="hover:bg-gray-50">
              {row.map((cell, j) => (
                <td
                  key={j}
                  className="px-4 py-2 text-sm text-gray-700 whitespace-nowrap"
                >
                  {cell === null || cell === undefined ? (
                    <span className="text-gray-400 italic">null</span>
                  ) : (
                    String(cell)
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function BarView({ columns, rows }) {
  const data = rowsToChartData(columns, rows)
  const xKey = columns[0]?.name
  const yKeys = columns.slice(1).map(c => c.name)
  if (!xKey || yKeys.length === 0) {
    return <p className="text-sm text-gray-500 italic">Need at least two columns to render a bar chart.</p>
  }
  return (
    <div className="bg-white shadow rounded-lg p-4" style={{ height: 420 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 16, right: 24, bottom: 48, left: 16 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={xKey} angle={-30} textAnchor="end" interval={0} height={70} />
          <YAxis />
          <Tooltip />
          {yKeys.length > 1 && <Legend />}
          {yKeys.map((k, i) => (
            <Bar key={k} dataKey={k} fill={PIE_COLORS[i % PIE_COLORS.length]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function LineView({ columns, rows }) {
  const data = rowsToChartData(columns, rows)
  const xKey = columns[0]?.name
  const yKeys = columns.slice(1).map(c => c.name)
  if (!xKey || yKeys.length === 0) {
    return <p className="text-sm text-gray-500 italic">Need at least two columns to render a line chart.</p>
  }
  return (
    <div className="bg-white shadow rounded-lg p-4" style={{ height: 420 }}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 16, right: 24, bottom: 48, left: 16 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={xKey} angle={-30} textAnchor="end" interval={0} height={70} />
          <YAxis />
          <Tooltip />
          {yKeys.length > 1 && <Legend />}
          {yKeys.map((k, i) => (
            <Line
              key={k}
              type="monotone"
              dataKey={k}
              stroke={PIE_COLORS[i % PIE_COLORS.length]}
              dot={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

function PieView({ columns, rows }) {
  const data = rowsToChartData(columns, rows)
  const nameKey = columns[0]?.name
  const valueKey = columns[1]?.name
  if (!nameKey || !valueKey) {
    return <p className="text-sm text-gray-500 italic">Need two columns to render a pie chart.</p>
  }
  return (
    <div className="bg-white shadow rounded-lg p-4" style={{ height: 420 }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            dataKey={valueKey}
            nameKey={nameKey}
            outerRadius={140}
            label
          >
            {data.map((_, i) => (
              <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  )
}

export function ResultRenderer({ viz_type = 'table', columns = [], rows = [] }) {
  const [view, setView] = useState(viz_type)

  let body
  switch (view) {
    case 'bar':
      body = <BarView columns={columns} rows={rows} />
      break
    case 'line':
      body = <LineView columns={columns} rows={rows} />
      break
    case 'pie':
      body = <PieView columns={columns} rows={rows} />
      break
    default:
      body = <TableView columns={columns} rows={rows} />
  }

  const VIEWS = ['table', 'bar', 'line', 'pie']
  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <span className="text-xs text-gray-500 uppercase">View as:</span>
        {VIEWS.map(v => (
          <button
            key={v}
            onClick={() => setView(v)}
            className={`text-xs px-2 py-1 rounded ${
              view === v
                ? 'bg-blue-100 text-blue-700 font-medium'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {v}
          </button>
        ))}
      </div>
      {body}
    </div>
  )
}
