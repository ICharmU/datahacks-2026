export function formatPercent(v: number) {
  return `${Math.round(v * 100)}%`
}

export function formatTime(ts: string) {
  return new Date(ts).toLocaleString()
}

export function titleCase(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase())
}
