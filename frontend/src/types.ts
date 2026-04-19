export type ProductShell = "fleet" | "grower"
export type DataMode = "mock" | "real"
export type WrapperType = "aquaculture" | "beach" | "fishing" | "surf" | "ecosystem"
export type RecommendationAction = "go" | "caution" | "avoid" | "harvest" | "delay" | "sample" | "warn_buyers"

export interface ApiEnvelope<T> {
  ok: boolean
  message: string
  data: T
}

export interface AppConfig {
  env: string
  use_mock_data: boolean
  wrappers: WrapperType[]
}

export interface ShellSummaryCard {
  title: string
  value: string
  delta?: string
  tone: "neutral" | "good" | "warn" | "bad"
}


export interface ZonePoint {
  location_id: string
  name: string
  lat: number
  lon: number
  risk_score: number
  risk_bucket: string
  uncertainty_score?: number | null
  confidence_score?: number | null
  opportunity_score?: number | null
  recommendation?: RecommendationAction
  region_name?: string
  latest_signal_summary?: string
}

export interface TimePoint {
  t: string
  value: number
}

export interface FactorContribution {
  name: string
  direction: "up" | "down"
  magnitude: number
  description?: string
}

export interface AdvisoryItem {
  id: string
  title: string
  severity: "info" | "watch" | "warning"
  source: string
  updated_at: string
  summary: string
}

export interface SpeciesSignal {
  species: string
  opportunity_score: number
  toxicity_risk_score: number
  shift_signal: "stable" | "northward" | "offshore" | "declining" | "uncertain"
}

export interface OpsMetric {
  label: string
  value: string
  help_text?: string
}

export interface FleetZoneDetail {
  location_id: string
  name: string
  port_name: string
  wrapper: "fishing"
  risk_bucket: string
  recommendation: RecommendationAction
  expected_value_label: string
  confidence_score: number
  forecast: TimePoint[]
  species_signals: SpeciesSignal[]
  top_factors: FactorContribution[]
  advisories: AdvisoryItem[]
  ops_metrics: OpsMetric[]
  recommended_action_text: string
  evidence: string[]
}

export interface FleetRecommendationRow {
  rank: number
  zone_id: string
  zone_name: string
  target_species: string
  recommendation: RecommendationAction
  opportunity_score: number
  risk_score: number
  net_trip_score: number
  fuel_burden_label: string
  rationale: string
}

export interface FleetDashboard {
  generated_at_utc: string
  port_name: string
  target_species: string
  weather_summary: string
  shell: "fleet"
  summary_cards: ShellSummaryCard[]
  recommendations: FleetRecommendationRow[]
  map_points: ZonePoint[]
  alerts: AdvisoryItem[]
}

export interface GrowerSiteSummary {
  site_id: string
  site_name: string
  lat: number
  lon: number
  recommendation: RecommendationAction
  risk_score: number
  harvest_window_label: string
  confidence_score: number
  latest_signal_summary: string
}

export interface SamplingPriority {
  label: string
  priority: "high" | "medium" | "low"
  reason: string
}

export interface GrowerSiteDetail {
  site_id: string
  site_name: string
  recommendation: RecommendationAction
  confidence_score: number
  harvest_window_label: string
  forecast: TimePoint[]
  top_factors: FactorContribution[]
  advisories: AdvisoryItem[]
  sampling_priorities: SamplingPriority[]
  buyer_warning_recommended: boolean
  recommended_action_text: string
  evidence: string[]
}

export interface GrowerDashboard {
  generated_at_utc: string
  region_name: string
  shell: "grower"
  summary_cards: ShellSummaryCard[]
  sites: GrowerSiteSummary[]
  alerts: AdvisoryItem[]
}

export interface MapRiskResponse {
  generated_at_utc: string
  shell: ProductShell
  wrapper: WrapperType
  horizon: string
  locations: ZonePoint[]
}

export interface ScienceDatasetItem {
  key: string
  name: string
  role: string
  status: "connected" | "mocked" | "planned"
  granularity: string
}

export interface ModelRunSummary {
  run_id: string
  name: string
  status: "success" | "running" | "failed"
  target: string
  score_label: string
  score_value: string
  notes: string
}

export interface ScienceSummary {
  datasets: ScienceDatasetItem[]
  model_runs: ModelRunSummary[]
  architecture_cards: ShellSummaryCard[]
}

export interface PipelineSourceStatus {
  source: string
  status: "ready" | "running" | "pending" | "error"
  last_updated: string
  note: string
}

export interface PipelineStatus {
  generated_at_utc: string
  bronze: PipelineSourceStatus[]
  silver: PipelineSourceStatus[]
  gold: PipelineSourceStatus[]
  serving: PipelineSourceStatus[]
}

export interface ShellSwitcherOption {
  key: ProductShell
  label: string
  subtitle: string
}
