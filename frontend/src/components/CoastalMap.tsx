import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet"
import type { ZonePoint } from "../types"
import { riskTone } from "../utils/colors"
import "leaflet/dist/leaflet.css"

export function CoastalMap({ points }: { points: ZonePoint[] }) {
  return (
    <div className="panel map-panel">
      <div className="panel__title">Coastal map</div>
      <div className="map-frame">
        <MapContainer center={[32.75, -117.24]} zoom={9} scrollWheelZoom={false} style={{ height: "100%", width: "100%" }}>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {points.map((point) => (
            <CircleMarker
              key={point.location_id}
              center={[point.lat, point.lon]}
              radius={8 + Math.round((point.opportunity_score ?? point.risk_score) * 8)}
              pathOptions={{ color: riskTone(point.risk_score), fillColor: riskTone(point.risk_score), fillOpacity: 0.75 }}
            >
              <Popup>
                <div>
                  <strong>{point.name}</strong>
                  <div>Risk: {Math.round(point.risk_score * 100)}</div>
                  {point.opportunity_score != null && <div>Opportunity: {Math.round(point.opportunity_score * 100)}</div>}
                  {point.recommendation && <div>Action: {point.recommendation}</div>}
                </div>
              </Popup>
            </CircleMarker>
          ))}
        </MapContainer>
      </div>
    </div>
  )
}
