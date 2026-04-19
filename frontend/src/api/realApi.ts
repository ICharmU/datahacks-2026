import { APP_CONFIG } from "../config"
import { getJson } from "./client"
import type {
  ApiEnvelope,
  AppConfig,
  FleetDashboard,
  FleetZoneDetail,
  GrowerDashboard,
  GrowerSiteDetail,
  MapRiskResponse,
  PipelineStatus,
  ProductShell,
  ScienceSummary,
  WrapperType,
} from "../types"

async function getEnvelope<T>(path: string): Promise<T> {
  const payload = await getJson<ApiEnvelope<T>>(`${APP_CONFIG.apiBaseUrl}${path}`)
  return payload.data
}

export const realApi = {
  getConfig(): Promise<AppConfig> {
    return getEnvelope<AppConfig>("/config/")
  },

  // The following are future-facing product routes. Keep the contract stable and gradually fill them in on Django.
  getFleetDashboard(portName = "San Diego", targetSpecies = "Market squid"): Promise<FleetDashboard> {
    return getEnvelope<FleetDashboard>(`/product/fleet/dashboard/?port=${encodeURIComponent(portName)}&species=${encodeURIComponent(targetSpecies)}`)
  },

  getFleetZoneDetail(zoneId: string): Promise<FleetZoneDetail> {
    return getEnvelope<FleetZoneDetail>(`/product/fleet/zones/${encodeURIComponent(zoneId)}/`)
  },

  getGrowerDashboard(region = "Southern California"): Promise<GrowerDashboard> {
    return getEnvelope<GrowerDashboard>(`/product/grower/dashboard/?region=${encodeURIComponent(region)}`)
  },

  getGrowerSiteDetail(siteId: string): Promise<GrowerSiteDetail> {
    return getEnvelope<GrowerSiteDetail>(`/product/grower/sites/${encodeURIComponent(siteId)}/`)
  },

  getMap(shell: ProductShell, wrapper: WrapperType, horizon: string): Promise<MapRiskResponse> {
    return getEnvelope<MapRiskResponse>(`/risk/map/?shell=${encodeURIComponent(shell)}&wrapper=${encodeURIComponent(wrapper)}&horizon=${encodeURIComponent(horizon)}`)
  },

  getScienceSummary(): Promise<ScienceSummary> {
    return getEnvelope<ScienceSummary>("/product/science/summary/")
  },

  getPipelineStatus(): Promise<PipelineStatus> {
    return getEnvelope<PipelineStatus>("/ingestion/pipeline-status/")
  },
}
