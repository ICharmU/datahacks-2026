import type {
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
import {
  buildMockMap,
  mockFleetDashboard,
  mockFleetZoneDetails,
  mockGrowerDashboard,
  mockGrowerSiteDetails,
  mockPipelineStatus,
  mockScienceSummary,
} from "./mockDatabase"

function delay<T>(value: T, ms = 180): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms))
}

export const mockApi = {
  async getConfig(): Promise<AppConfig> {
    return delay({ env: "dev", use_mock_data: true, wrappers: ["beach", "fishing", "surf", "ecosystem"] })
  },

  async getFleetDashboard(_portName = "San Diego", _targetSpecies = "Market squid"): Promise<FleetDashboard> {
    return delay(mockFleetDashboard)
  },

  async getFleetZoneDetail(zoneId: string): Promise<FleetZoneDetail> {
    return delay(mockFleetZoneDetails[zoneId] ?? mockFleetZoneDetails["zone-b12"])
  },

  async getGrowerDashboard(_region = "Southern California"): Promise<GrowerDashboard> {
    return delay(mockGrowerDashboard)
  },

  async getGrowerSiteDetail(siteId: string): Promise<GrowerSiteDetail> {
    return delay(mockGrowerSiteDetails[siteId] ?? mockGrowerSiteDetails["farm-sd-01"])
  },

  async getMap(shell: ProductShell, wrapper: WrapperType, horizon: string): Promise<MapRiskResponse> {
    return delay(buildMockMap(shell, wrapper, horizon))
  },

  async getScienceSummary(): Promise<ScienceSummary> {
    return delay(mockScienceSummary)
  },

  async getPipelineStatus(): Promise<PipelineStatus> {
    return delay(mockPipelineStatus)
  },
}
