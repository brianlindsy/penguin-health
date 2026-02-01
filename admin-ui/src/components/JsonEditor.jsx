import { useState, useEffect } from 'react'

export function JsonEditor({ value, onChange, label }) {
  const [text, setText] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    setText(JSON.stringify(value, null, 2))
  }, [value])

  const handleChange = (e) => {
    const raw = e.target.value
    setText(raw)
    try {
      const parsed = JSON.parse(raw)
      setError('')
      onChange(parsed)
    } catch {
      setError('Invalid JSON')
    }
  }

  return (
    <div>
      {label && <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>}
      <textarea
        value={text}
        onChange={handleChange}
        rows={12}
        className={`w-full font-mono text-sm px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 ${
          error ? 'border-red-400' : 'border-gray-300'
        }`}
      />
      {error && <p className="text-red-600 text-xs mt-1">{error}</p>}
    </div>
  )
}
