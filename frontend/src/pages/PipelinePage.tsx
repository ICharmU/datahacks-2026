import { useEffect, useState } from "react"
import { getApi } from "../api"
import type { DataMode, PipelineSourceStatus, PipelineStatus } from "../types"

function StatusSection({ title, rows }: { title: string; rows: PipelineSourceStatus[] }) {
  return (
    <div className="panel">
      <div className="panel__title">{title}</div>
      <div className="metric-list">
        {rows.map((row) => (
          <div key={row.source} className="metric-list__item">
            <strong>{row.source}</strong>
            <span>{row.status}</span>
            <small>{row.note}</small>
            <small>{new Date(row.last_updated).toLocaleString()}</small>
          </div>
        ))}
      </div>
    </div>
  )
}

export function PipelinePage({ dataMode }: { dataMode: DataMode }) {
  const [data, setData] = useState<PipelineStatus | null>(null)

  useEffect(() => {
    getApi(dataMode).getPipelineStatus().then(setData)
  }, [dataMode])

  if (!data) return <div className="loading-card">Loading pipeline status…</div>

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Pipeline surface</div>
          <h1>Backend and serving readiness</h1>
          <p>Mocked pipeline-state board aligned to Bronze / Silver / Gold / Serving layers.</p>
        </div>
      </section>
      <div className="two-col-grid">
        <StatusSection title="Bronze" rows={data.bronze} />
        <StatusSection title="Silver" rows={data.silver} />
        <StatusSection title="Gold" rows={data.gold} />
        <StatusSection title="Serving" rows={data.serving} />
      </div>
    </div>
  )
}
