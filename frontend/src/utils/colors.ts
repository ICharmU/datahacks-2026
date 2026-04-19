export function riskTone(score: number) {
  if (score >= 0.75) return "var(--danger)"
  if (score >= 0.5) return "var(--warning)"
  if (score >= 0.3) return "var(--caution)"
  return "var(--success)"
}
