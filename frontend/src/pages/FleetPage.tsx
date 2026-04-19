import { useEffect, useState } from "react"
import { getApi } from "../api"
import type { DataMode, FleetDashboard } from "../types"
import { SummaryCardGrid } from "../components/SummaryCardGrid"
import { ZoneRecommendationTable } from "../components/ZoneRecommendationTable"
import { CoastalMap } from "../components/CoastalMap"
import { AdvisoryList } from "../components/AdvisoryList"

export function FleetPage({ dataMode }: { dataMode: DataMode }) {
  const [data, setData] = useState<FleetDashboard | null>(null)

  useEffect(() => {
    getApi(dataMode).getFleetDashboard().then(setData)
  }, [dataMode])

  if (!data) return <div className="loading-card">Loading fleet dashboard…</div>

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Fleet shell</div>
          <h1>{data.port_name} operator dashboard</h1>
          <p>{data.weather_summary}</p>
        </div>
        <div className="pill-group">
          <span className="pill">Target species: {data.target_species}</span>
          <span className="pill">Generated: {new Date(data.generated_at_utc).toLocaleString()}</span>
        </div>
      </section>

      <SummaryCardGrid cards={data.summary_cards} />

      <section className="two-col-grid map-and-alerts">
        <CoastalMap points={data.map_points} />
        <AdvisoryList advisories={data.alerts} />
      </section>

      <ZoneRecommendationTable rows={data.recommendations} />
    </div>
  )
}
