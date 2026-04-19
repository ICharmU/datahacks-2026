import { titleCase } from "../utils/format"

export function RiskBadge({ label }: { label: string }) {
  return <span className="risk-badge">{titleCase(label)}</span>
}
