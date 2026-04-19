import { useEffect, useMemo, useState } from "react"
import { Link } from "react-router-dom"
import { getApi } from "../api"
import type { DataMode, GrowerDashboard, ZonePoint } from "../types"
import { SummaryCardGrid } from "../components/SummaryCardGrid"
import { AdvisoryList } from "../components/AdvisoryList"
import { CoastalMap } from "../components/CoastalMap"
import { RiskBadge } from "../components/RiskBadge"

export function GrowerPage({ dataMode }: { dataMode: DataMode }) {
  const [data, setData] = useState<GrowerDashboard | null>(null)
  const [selectedSiteId, setSelectedSiteId] = useState<string | null>(null)

  useEffect(() => {
    getApi(dataMode).getGrowerDashboard().then((payload) => {
      setData(payload)
      setSelectedSiteId(payload.sites[0]?.site_id ?? null)
    })
  }, [dataMode])

  const mapPoints = useMemo<ZonePoint[]>(() => {
    if (!data) return []
    return data.sites.map((site) => ({
      location_id: site.site_id,
      name: site.site_name,
      lat: site.lat,
      lon: site.lon,
      risk_score: site.risk_score,
      risk_bucket: site.recommendation,
      confidence_score: site.confidence_score,
      recommendation: site.recommendation,
      latest_signal_summary: site.latest_signal_summary,
    }))
  }, [data])

  if (!data) return <div className="loading-card">Loading grower dashboard…</div>

  const selectedSite = data.sites.find((s) => s.site_id === selectedSiteId) ?? data.sites[0]

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Aquaculture shell</div>
          <h1>{data.region_name}</h1>
          <p>Harvest-risk intelligence for shellfish growers and nearshore aquaculture operators.</p>
        </div>
      </section>

      <SummaryCardGrid cards={data.summary_cards} />

      <div className="two-col-grid">
        <CoastalMap points={mapPoints} title="Aquaculture watch map" selectedId={selectedSiteId} onSelect={setSelectedSiteId} />

        <div className="panel">
          <div className="panel__title">Selected site</div>
          {selectedSite ? (
            <div className="metric-list">
              <div className="metric-list__item">
                <strong>{selectedSite.site_name}</strong>
                <RiskBadge label={selectedSite.recommendation} />
                <span>{selectedSite.harvest_window_label}</span>
                <small>{selectedSite.latest_signal_summary}</small>
                <Link to={`/grower/site/${selectedSite.site_id}`}>Open site detail</Link>
              </div>
            </div>
          ) : (
            <div className="muted-text">Select a site on the map.</div>
          )}
        </div>
      </div>

      <div className="two-col-grid">
        <div className="panel">
          <div className="panel__title">Site recommendations</div>
          <div className="site-list">
            {data.sites.map((site) => (
              <button
                key={site.site_id}
                type="button"
                className="site-card"
                onClick={() => setSelectedSiteId(site.site_id)}
              >
                <strong>{site.site_name}</strong>
                <span>{site.harvest_window_label}</span>
                <span>Action: {site.recommendation}</span>
                <small>{site.latest_signal_summary}</small>
              </button>
            ))}
          </div>
        </div>
        <AdvisoryList advisories={data.alerts} />
      </div>
    </div>
  )
}