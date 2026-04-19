import type { AdvisoryItem } from "../types"
import { formatTime } from "../utils/format"

export function AdvisoryList({ advisories }: { advisories: AdvisoryItem[] }) {
  return (
    <div className="panel">
      <div className="panel__title">Advisories + alerts</div>
      <div className="advisory-list">
        {advisories.map((advisory) => (
          <div key={advisory.id} className={`advisory advisory--${advisory.severity}`}>
            <div className="advisory__meta">
              <strong>{advisory.title}</strong>
              <span>{advisory.source}</span>
            </div>
            <p>{advisory.summary}</p>
            <small>{formatTime(advisory.updated_at)}</small>
          </div>
        ))}
      </div>
    </div>
  )
}
