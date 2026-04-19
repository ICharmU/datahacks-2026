import { Link } from "react-router-dom"
import type { FleetRecommendationRow } from "../types"

export function ZoneRecommendationTable({ rows }: { rows: FleetRecommendationRow[] }) {
  return (
    <div className="panel">
      <div className="panel__title">Ranked fishing zones</div>
      <div className="table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Zone</th>
              <th>Target</th>
              <th>Action</th>
              <th>Opportunity</th>
              <th>Risk</th>
              <th>Net score</th>
              <th>Fuel</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.zone_id}>
                <td>{row.rank}</td>
                <td>
                  <Link to={`/fleet/zone/${row.zone_id}`}>{row.zone_name}</Link>
                  <div className="muted-text">{row.rationale}</div>
                </td>
                <td>{row.target_species}</td>
                <td>{row.recommendation}</td>
                <td>{Math.round(row.opportunity_score * 100)}</td>
                <td>{Math.round(row.risk_score * 100)}</td>
                <td>{Math.round(row.net_trip_score * 100)}</td>
                <td>{row.fuel_burden_label}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
