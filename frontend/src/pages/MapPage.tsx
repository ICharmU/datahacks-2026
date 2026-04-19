import { useEffect, useMemo, useState } from "react"
import { getApi } from "../api"
import type { DataMode, MapRiskResponse, ProductShell, WrapperType, ZonePoint } from "../types"
import { CoastalMap } from "../components/CoastalMap"
import { WrapperTabs } from "../components/WrapperTabs"

export function MapPage({ dataMode, shell }: { dataMode: DataMode; shell: ProductShell }) {
  const [wrapper, setWrapper] = useState<WrapperType>(shell === "grower" ? "aquaculture" : "fishing")
  const [horizon, setHorizon] = useState("24h")
  const [data, setData] = useState<MapRiskResponse | null>(null)
  const [selectedId, setSelectedId] = useState<string | null>(null)

  useEffect(() => {
    setWrapper(shell === "grower" ? "aquaculture" : "fishing")
  }, [shell])

  useEffect(() => {
    getApi(dataMode).getMap(shell, wrapper, horizon).then((payload) => {
      setData(payload)
      setSelectedId(payload.locations[0]?.location_id ?? null)
    })
  }, [dataMode, shell, wrapper, horizon])

  const selectedPoint = useMemo<ZonePoint | undefined>(
    () => data?.locations.find((p) => p.location_id === selectedId),
    [data, selectedId]
  )

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Shared map surface</div>
          <h1>Regional coastal intelligence map</h1>
          <p>Compare aquaculture and beach risk surfaces from the same serving backend.</p>
        </div>
        <div className="pill-group">
          {(["24h", "72h"] as const).map((option) => (
            <button key={option} className={`pill-button ${horizon === option ? "active" : ""}`} onClick={() => setHorizon(option)}>
              {option}
            </button>
          ))}
        </div>
      </section>

      <WrapperTabs shell={shell} value={wrapper} onChange={setWrapper} />

      <div className="two-col-grid">
        {!data ? (
          <div className="loading-card">Loading map…</div>
        ) : (
          <CoastalMap points={data.locations} selectedId={selectedId} onSelect={setSelectedId} title="Interactive coastal risk map" />
        )}

        <div className="panel">
          <div className="panel__title">Selected location</div>
          {selectedPoint ? (
            <div className="metric-list">
              <div className="metric-list__item">
                <strong>{selectedPoint.name}</strong>
                <span>Risk {Math.round(selectedPoint.risk_score * 100)}</span>
                {selectedPoint.uncertainty_score != null && (
                  <span>Uncertainty {Math.round(selectedPoint.uncertainty_score * 100)}</span>
                )}
                {selectedPoint.recommendation && <small>Recommended action: {selectedPoint.recommendation}</small>}
              </div>
            </div>
          ) : (
            <div className="muted-text">Select a point on the map.</div>
          )}
        </div>
      </div>
    </div>
  )
}