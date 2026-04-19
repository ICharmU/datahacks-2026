import type { FactorContribution } from "../types"

export function FactorList({ factors }: { factors: FactorContribution[] }) {
  return (
    <div className="panel">
      <div className="panel__title">Top contributing factors</div>
      <div className="factor-list">
        {factors.map((factor) => (
          <div key={factor.name} className="factor-item">
            <div>
              <strong>{factor.name.replaceAll("_", " ")}</strong>
              {factor.description && <p>{factor.description}</p>}
            </div>
            <div className={`factor-pill ${factor.direction}`}>
              {factor.direction} · {(factor.magnitude * 100).toFixed(0)}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
