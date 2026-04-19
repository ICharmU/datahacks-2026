import { useEffect, useState } from "react"
import { getApi } from "../api"
import type { DataMode, MapRiskResponse, ProductShell, WrapperType } from "../types"
import { CoastalMap } from "../components/CoastalMap"
import { WrapperTabs } from "../components/WrapperTabs"

export function MapPage({ dataMode, shell }: { dataMode: DataMode; shell: ProductShell }) {
  const [wrapper, setWrapper] = useState<WrapperType>(shell === "fleet" ? "fishing" : "ecosystem")
  const [horizon, setHorizon] = useState("24h")
  const [data, setData] = useState<MapRiskResponse | null>(null)

  useEffect(() => {
    getApi(dataMode).getMap(shell, wrapper, horizon).then(setData)
  }, [dataMode, shell, wrapper, horizon])

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Shared map surface</div>
          <h1>Regional risk map</h1>
          <p>Use the same coastal intelligence backend across fleet and grower workflows.</p>
        </div>
        <div className="pill-group">
          {(["24h", "72h"] as const).map((option) => (
            <button key={option} className={`pill-button ${horizon === option ? "active" : ""}`} onClick={() => setHorizon(option)}>
              {option}
            </button>
          ))}
        </div>
      </section>

      <WrapperTabs value={wrapper} onChange={setWrapper} />

      {!data ? <div className="loading-card">Loading map…</div> : <CoastalMap points={data.locations} />}
    </div>
  )
}
