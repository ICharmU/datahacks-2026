import { useEffect, useState } from "react"
import { useParams } from "react-router-dom"
import { getApi } from "../api"
import type { DataMode, GrowerSiteDetail } from "../types"
import { ForecastChartPlaceholder } from "../components/ForecastChartPlaceholder"
import { FactorList } from "../components/FactorList"
import { AdvisoryList } from "../components/AdvisoryList"
import { RiskBadge } from "../components/RiskBadge"

export function GrowerSiteDetailPage({ dataMode }: { dataMode: DataMode }) {
  const { siteId = "aq_san_diego_bay" } = useParams()
  const [data, setData] = useState<GrowerSiteDetail | null>(null)

  useEffect(() => {
    getApi(dataMode).getGrowerSiteDetail(siteId).then(setData)
  }, [dataMode, siteId])

  if (!data) return <div className="loading-card">Loading grower site detail…</div>

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Aquaculture site detail</div>
          <h1>{data.site_name}</h1>
          <p>{data.recommended_action_text}</p>
        </div>
        <div className="pill-group">
          <RiskBadge label={data.recommendation} />
          <span className="pill">{data.harvest_window_label}</span>
          <span className="pill">Confidence {Math.round(data.confidence_score * 100)}%</span>
        </div>
      </section>

      <div className="two-col-grid">
        <ForecastChartPlaceholder data={data.forecast} title="Aquaculture risk trajectory" />
        <FactorList factors={data.top_factors} />
      </div>

      <div className="two-col-grid">
        <div className="panel">
          <div className="panel__title">Sampling priorities</div>
          <div className="metric-list">
            {data.sampling_priorities.map((item) => (
              <div key={item.label} className="metric-list__item">
                <strong>{item.label}</strong>
                <span>{item.priority}</span>
                <small>{item.reason}</small>
              </div>
            ))}
          </div>
        </div>
        <AdvisoryList advisories={data.advisories} />
      </div>

      <div className="panel">
        <div className="panel__title">Evidence trail</div>
        <ul className="evidence-list">
          {data.evidence.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      </div>
    </div>
  )
}