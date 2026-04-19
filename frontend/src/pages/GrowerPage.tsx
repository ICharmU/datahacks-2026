import { useEffect, useState } from "react"
import { Link } from "react-router-dom"
import { getApi } from "../api"
import type { DataMode, GrowerDashboard } from "../types"
import { SummaryCardGrid } from "../components/SummaryCardGrid"
import { AdvisoryList } from "../components/AdvisoryList"

export function GrowerPage({ dataMode }: { dataMode: DataMode }) {
  const [data, setData] = useState<GrowerDashboard | null>(null)

  useEffect(() => {
    getApi(dataMode).getGrowerDashboard().then(setData)
  }, [dataMode])

  if (!data) return <div className="loading-card">Loading grower dashboard…</div>

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Grower shell</div>
          <h1>{data.region_name}</h1>
          <p>Operational harvest-risk intelligence for nearshore growers and shellfish operators.</p>
        </div>
      </section>

      <SummaryCardGrid cards={data.summary_cards} />

      <div className="two-col-grid">
        <div className="panel">
          <div className="panel__title">Site recommendations</div>
          <div className="site-list">
            {data.sites.map((site) => (
              <Link key={site.site_id} className="site-card" to={`/grower/site/${site.site_id}`}>
                <strong>{site.site_name}</strong>
                <span>{site.harvest_window_label}</span>
                <span>Action: {site.recommendation}</span>
                <small>{site.latest_signal_summary}</small>
              </Link>
            ))}
          </div>
        </div>
        <AdvisoryList advisories={data.alerts} />
      </div>
    </div>
  )
}
