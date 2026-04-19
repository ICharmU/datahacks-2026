import { useEffect, useState } from "react"
import { useParams } from "react-router-dom"
import { getApi } from "../api"
import type { DataMode, FleetZoneDetail } from "../types"
import { ForecastChartPlaceholder } from "../components/ForecastChartPlaceholder"
import { FactorList } from "../components/FactorList"
import { AdvisoryList } from "../components/AdvisoryList"
import { RiskBadge } from "../components/RiskBadge"

export function FleetZoneDetailPage({ dataMode }: { dataMode: DataMode }) {
  const { zoneId = "zone-b12" } = useParams()
  const [data, setData] = useState<FleetZoneDetail | null>(null)

  useEffect(() => {
    getApi(dataMode).getFleetZoneDetail(zoneId).then(setData)
  }, [dataMode, zoneId])

  if (!data) return <div className="loading-card">Loading zone detail…</div>

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Zone decision detail</div>
          <h1>{data.name}</h1>
          <p>{data.recommended_action_text}</p>
        </div>
        <div className="pill-group">
          <RiskBadge label={data.risk_bucket} />
          <span className="pill">{data.expected_value_label}</span>
          <span className="pill">Confidence {Math.round(data.confidence_score * 100)}%</span>
        </div>
      </section>

      <div className="two-col-grid">
        <ForecastChartPlaceholder data={data.forecast} title="Risk trajectory" />
        <FactorList factors={data.top_factors} />
      </div>

      <div className="two-col-grid">
        <div className="panel">
          <div className="panel__title">Species signals</div>
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Species</th>
                  <th>Opportunity</th>
                  <th>Toxicity risk</th>
                  <th>Shift signal</th>
                </tr>
              </thead>
              <tbody>
                {data.species_signals.map((signal) => (
                  <tr key={signal.species}>
                    <td>{signal.species}</td>
                    <td>{Math.round(signal.opportunity_score * 100)}</td>
                    <td>{Math.round(signal.toxicity_risk_score * 100)}</td>
                    <td>{signal.shift_signal}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="panel">
          <div className="panel__title">Operational metrics</div>
          <div className="metric-list">
            {data.ops_metrics.map((metric) => (
              <div key={metric.label} className="metric-list__item">
                <strong>{metric.label}</strong>
                <span>{metric.value}</span>
                {metric.help_text && <small>{metric.help_text}</small>}
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="two-col-grid">
        <AdvisoryList advisories={data.advisories} />
        <div className="panel">
          <div className="panel__title">Evidence trail</div>
          <ul className="evidence-list">
            {data.evidence.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  )
}
