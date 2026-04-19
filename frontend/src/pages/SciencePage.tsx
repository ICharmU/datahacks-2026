import { useEffect, useState } from "react"
import { getApi } from "../api"
import type { DataMode, ScienceSummary } from "../types"
import { SummaryCardGrid } from "../components/SummaryCardGrid"

export function SciencePage({ dataMode }: { dataMode: DataMode }) {
  const [data, setData] = useState<ScienceSummary | null>(null)

  useEffect(() => {
    getApi(dataMode).getScienceSummary().then(setData)
  }, [dataMode])

  if (!data) return <div className="loading-card">Loading science summary…</div>

  return (
    <div className="page-stack">
      <section className="page-header-card">
        <div>
          <div className="eyebrow">Science + sponsor surface</div>
          <h1>Shared model and data backbone</h1>
          <p>Frontend-ready summary of the dataset stack, model runs, and sponsor-facing architecture.</p>
        </div>
      </section>

      <SummaryCardGrid cards={data.architecture_cards} />

      <div className="two-col-grid">
        <div className="panel">
          <div className="panel__title">Datasets</div>
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Status</th>
                  <th>Granularity</th>
                </tr>
              </thead>
              <tbody>
                {data.datasets.map((item) => (
                  <tr key={item.key}>
                    <td>{item.name}</td>
                    <td>{item.role}</td>
                    <td>{item.status}</td>
                    <td>{item.granularity}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="panel">
          <div className="panel__title">Model runs</div>
          <div className="metric-list">
            {data.model_runs.map((run) => (
              <div key={run.run_id} className="metric-list__item">
                <strong>{run.name}</strong>
                <span>{run.target}</span>
                <span>{run.score_label}: {run.score_value}</span>
                <small>{run.notes}</small>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
