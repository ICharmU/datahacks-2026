import { MapContainer, TileLayer, CircleMarker, Popup, useMap } from "react-leaflet"
import type { ZonePoint } from "../types"
import { riskTone } from "../utils/colors"
import "leaflet/dist/leaflet.css"

function FitToPoints({ points }: { points: ZonePoint[] }) {
  const map = useMap()

  if (points.length > 0) {
    const bounds = points.map((p) => [p.lat, p.lon] as [number, number])
    map.fitBounds(bounds, { padding: [30, 30] })
  }

  return null
}

export function CoastalMap({
  points,
  title = "Coastal map",
  selectedId,
  onSelect,
}: {
  points: ZonePoint[]
  title?: string
  selectedId?: string | null
  onSelect?: (locationId: string) => void
}) {
  return (
    <div className="panel map-panel">
      <div className="panel__title">{title}</div>
      <div className="map-frame">
        <MapContainer center={[32.75, -117.24]} zoom={9} scrollWheelZoom={false} style={{ height: "100%", width: "100%" }}>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <FitToPoints points={points} />
          {points.map((point) => {
            const selected = point.location_id === selectedId
            const radiusBase = point.confidence_score != null ? point.confidence_score : (point.opportunity_score ?? point.risk_score)
            const radius = 8 + Math.round(radiusBase * 10)

            return (
              <CircleMarker
                key={point.location_id}
                center={[point.lat, point.lon]}
                radius={radius}
                eventHandlers={{
                  click: () => onSelect?.(point.location_id),
                }}
                pathOptions={{
                  color: selected ? "#ffffff" : riskTone(point.risk_score),
                  weight: selected ? 3 : 1,
                  fillColor: riskTone(point.risk_score),
                  fillOpacity: 0.8,
                }}
              >
                <Popup>
                  <div>
                    <strong>{point.name}</strong>
                    <div>Risk: {Math.round(point.risk_score * 100)}</div>
                    {point.confidence_score != null && <div>Confidence: {Math.round(point.confidence_score * 100)}%</div>}
                    {point.opportunity_score != null && <div>Opportunity: {Math.round(point.opportunity_score * 100)}</div>}
                    {point.recommendation && <div>Action: {point.recommendation}</div>}
                    {point.region_name && <div>Region: {point.region_name}</div>}
                    {point.latest_signal_summary && <div>{point.latest_signal_summary}</div>}
                  </div>
                </Popup>
              </CircleMarker>
            )
          })}
        </MapContainer>
      </div>
      <div className="muted-text">Marker size reflects confidence or signal strength. Color reflects modeled risk.</div>
    </div>
  )
}