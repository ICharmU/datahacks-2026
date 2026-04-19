import type {
  AdvisoryItem,
  FleetDashboard,
  FleetZoneDetail,
  GrowerDashboard,
  GrowerSiteDetail,
  MapRiskResponse,
  PipelineStatus,
  ScienceSummary,
  WrapperType,
} from "../types"

const now = new Date()
const iso = (hours = 0) => new Date(now.getTime() + hours * 3600 * 1000).toISOString()

export const mockAlerts: AdvisoryItem[] = [
  {
    id: "adv-1",
    title: "Elevated domoic-acid watch",
    severity: "warning",
    source: "CDPH + internal blend",
    updated_at: iso(-2),
    summary: "Nearshore toxicity propensity is elevated relative to the recent baseline in southern coastal segments.",
  },
  {
    id: "adv-2",
    title: "Biological productivity spike",
    severity: "watch",
    source: "CalCOFI / Ocean Color fusion",
    updated_at: iso(-5),
    summary: "Chlorophyll-linked productivity increased over the last 24 hours and now resembles prior moderate-risk periods.",
  },
  {
    id: "adv-3",
    title: "Transport conditions favor persistence",
    severity: "info",
    source: "CDIP + tides/currents",
    updated_at: iso(-3),
    summary: "Wave and coastal transport conditions may sustain nearshore exposure over the next day.",
  },
]

export const mockFleetDashboard: FleetDashboard = {
  generated_at_utc: iso(),
  port_name: "San Diego",
  target_species: "Market squid",
  weather_summary: "Manageable winds, moderate swell, elevated biological risk in selected nearshore corridors.",
  shell: "fleet",
  summary_cards: [
    { title: "Best zone today", value: "B12 Offshore Edge", delta: "+11 vs yesterday", tone: "good" },
    { title: "Avoid zones", value: "2", delta: "1 newly degraded", tone: "warn" },
    { title: "Net trip score", value: "78/100", delta: "Risk-adjusted", tone: "neutral" },
    { title: "Fuel burden", value: "Low-Moderate", delta: "Within captain threshold", tone: "good" },
  ],
  recommendations: [
    {
      rank: 1,
      zone_id: "zone-b12",
      zone_name: "B12 Offshore Edge",
      target_species: "Market squid",
      recommendation: "go",
      opportunity_score: 0.84,
      risk_score: 0.34,
      net_trip_score: 0.78,
      fuel_burden_label: "Low-Moderate",
      rationale: "Best combined opportunity and manageable toxicity exposure under today’s transport conditions.",
    },
    {
      rank: 2,
      zone_id: "zone-c09",
      zone_name: "C09 North Drift",
      target_species: "Market squid",
      recommendation: "caution",
      opportunity_score: 0.81,
      risk_score: 0.53,
      net_trip_score: 0.67,
      fuel_burden_label: "Moderate",
      rationale: "Attractive opportunity but elevated biological risk requires tighter catch-handling and verification.",
    },
    {
      rank: 3,
      zone_id: "zone-a07",
      zone_name: "A07 Nearshore Shelf",
      target_species: "Market squid",
      recommendation: "avoid",
      opportunity_score: 0.63,
      risk_score: 0.82,
      net_trip_score: 0.22,
      fuel_burden_label: "Low",
      rationale: "Close and cheap to reach, but elevated toxin and advisory persistence make the trip unattractive.",
    },
  ],
  map_points: [
    { location_id: "zone-b12", name: "B12 Offshore Edge", lat: 32.72, lon: -117.41, risk_score: 0.34, risk_bucket: "moderate", uncertainty_score: 0.12, opportunity_score: 0.84, recommendation: "go" },
    { location_id: "zone-c09", name: "C09 North Drift", lat: 32.89, lon: -117.33, risk_score: 0.53, risk_bucket: "elevated", uncertainty_score: 0.18, opportunity_score: 0.81, recommendation: "caution" },
    { location_id: "zone-a07", name: "A07 Nearshore Shelf", lat: 32.63, lon: -117.21, risk_score: 0.82, risk_bucket: "high", uncertainty_score: 0.10, opportunity_score: 0.63, recommendation: "avoid" },
    { location_id: "zone-d14", name: "D14 South Gradient", lat: 32.51, lon: -117.38, risk_score: 0.41, risk_bucket: "moderate", uncertainty_score: 0.16, opportunity_score: 0.71, recommendation: "go" },
  ],
  alerts: mockAlerts,
}

export const mockFleetZoneDetails: Record<string, FleetZoneDetail> = {
  "zone-b12": {
    location_id: "zone-b12",
    name: "B12 Offshore Edge",
    port_name: "San Diego",
    wrapper: "fishing",
    risk_bucket: "moderate",
    recommendation: "go",
    expected_value_label: "$4.8k expected trip value",
    confidence_score: 0.81,
    forecast: [
      { t: iso(0), value: 0.34 },
      { t: iso(12), value: 0.39 },
      { t: iso(24), value: 0.43 },
      { t: iso(48), value: 0.36 },
    ],
    species_signals: [
      { species: "Market squid", opportunity_score: 0.88, toxicity_risk_score: 0.31, shift_signal: "stable" },
      { species: "Sardine", opportunity_score: 0.56, toxicity_risk_score: 0.29, shift_signal: "offshore" },
      { species: "Anchovy", opportunity_score: 0.49, toxicity_risk_score: 0.27, shift_signal: "stable" },
    ],
    top_factors: [
      { name: "opportunity_vs_risk_blend", direction: "down", magnitude: 0.91, description: "Strong opportunity remains ahead of moderate bio-risk." },
      { name: "recent_transport_stability", direction: "down", magnitude: 0.64, description: "Offshore transport lowers persistence risk versus nearshore zones." },
      { name: "productivity_signal", direction: "up", magnitude: 0.42, description: "Productivity is elevated, but not to levels seen in the highest-risk zones." },
    ],
    advisories: mockAlerts,
    ops_metrics: [
      { label: "Transit time", value: "41 min", help_text: "Estimated from port reference and zone centroid." },
      { label: "Fuel burden", value: "Low-Moderate", help_text: "Relative ranking based on trip geometry." },
      { label: "Closure overlap", value: "None" },
      { label: "Uncertainty", value: "12%" },
    ],
    recommended_action_text: "Primary target zone today. Fish here first, verify handling protocol, and reassess after midday sensor refresh.",
    evidence: [
      "Target-species opportunity remains high in the latest fused distribution model.",
      "Offshore positioning reduces projected persistence of toxin exposure relative to nearshore corridors.",
      "No current closure overlap and manageable fuel burden preserve trip economics.",
    ],
  },
  "zone-c09": {
    location_id: "zone-c09",
    name: "C09 North Drift",
    port_name: "San Diego",
    wrapper: "fishing",
    risk_bucket: "elevated",
    recommendation: "caution",
    expected_value_label: "$4.1k expected trip value",
    confidence_score: 0.68,
    forecast: [
      { t: iso(0), value: 0.53 },
      { t: iso(12), value: 0.57 },
      { t: iso(24), value: 0.62 },
      { t: iso(48), value: 0.49 },
    ],
    species_signals: [
      { species: "Market squid", opportunity_score: 0.82, toxicity_risk_score: 0.55, shift_signal: "northward" },
      { species: "Sardine", opportunity_score: 0.61, toxicity_risk_score: 0.52, shift_signal: "stable" },
    ],
    top_factors: [
      { name: "northward_shift_signal", direction: "down", magnitude: 0.73, description: "Species opportunity remains decent as biomass shifts north." },
      { name: "historical_advisory_similarity", direction: "up", magnitude: 0.62, description: "Conditions resemble prior moderate-to-high risk episodes." },
    ],
    advisories: mockAlerts,
    ops_metrics: [
      { label: "Transit time", value: "54 min" },
      { label: "Fuel burden", value: "Moderate" },
      { label: "Closure overlap", value: "Watch nearby" },
      { label: "Uncertainty", value: "18%" },
    ],
    recommended_action_text: "Secondary option only. Use if the primary zone degrades, and tighten catch verification and toxin checks.",
    evidence: [
      "Opportunity remains viable but risk is materially higher than the top-ranked zone.",
      "Projected conditions worsen slightly over the next 24 hours.",
    ],
  },
  "zone-a07": {
    location_id: "zone-a07",
    name: "A07 Nearshore Shelf",
    port_name: "San Diego",
    wrapper: "fishing",
    risk_bucket: "high",
    recommendation: "avoid",
    expected_value_label: "$1.3k expected trip value",
    confidence_score: 0.88,
    forecast: [
      { t: iso(0), value: 0.82 },
      { t: iso(12), value: 0.88 },
      { t: iso(24), value: 0.91 },
      { t: iso(48), value: 0.79 },
    ],
    species_signals: [
      { species: "Market squid", opportunity_score: 0.63, toxicity_risk_score: 0.84, shift_signal: "declining" },
      { species: "Anchovy", opportunity_score: 0.44, toxicity_risk_score: 0.79, shift_signal: "uncertain" },
    ],
    top_factors: [
      { name: "domoic_propensity", direction: "up", magnitude: 0.92, description: "Nearshore biological-risk blend is strongly elevated." },
      { name: "persistence_transport", direction: "up", magnitude: 0.76, description: "Transport conditions favor continued nearshore exposure." },
      { name: "closure_history", direction: "up", magnitude: 0.67, description: "Historical analogs support elevated caution." },
    ],
    advisories: mockAlerts,
    ops_metrics: [
      { label: "Transit time", value: "19 min" },
      { label: "Fuel burden", value: "Low" },
      { label: "Closure overlap", value: "Potential advisory overlap" },
      { label: "Uncertainty", value: "10%" },
    ],
    recommended_action_text: "Avoid this zone today. Short transit time does not compensate for elevated toxin and persistence risk.",
    evidence: [
      "Modeled toxicity propensity is among the highest in the region.",
      "Historical advisory density and recent productivity spikes both point upward.",
      "Projected risk remains high through the next 24 hours.",
    ],
  },
}

export const mockGrowerDashboard: GrowerDashboard = {
  generated_at_utc: iso(),
  region_name: "Southern California Nearshore Farms",
  shell: "grower",
  summary_cards: [
    { title: "Harvest now", value: "2 sites", delta: "1 within premium window", tone: "good" },
    { title: "Delay / sample", value: "3 sites", delta: "Toxin risk rising", tone: "warn" },
    { title: "Buyer warnings", value: "1 recommended", delta: "Escalation pending", tone: "bad" },
    { title: "Confidence", value: "76%", delta: "Model blend", tone: "neutral" },
  ],
  sites: [
    {
      site_id: "farm-sd-01",
      site_name: "Coronado Kelp & Shell Site",
      lat: 32.66,
      lon: -117.21,
      recommendation: "harvest",
      risk_score: 0.28,
      harvest_window_label: "Harvest within 18 hours",
      confidence_score: 0.83,
      latest_signal_summary: "Low toxin propensity and favorable short harvest window.",
    },
    {
      site_id: "farm-sd-02",
      site_name: "North County Longline Site",
      lat: 33.01,
      lon: -117.31,
      recommendation: "sample",
      risk_score: 0.57,
      harvest_window_label: "Sample before release",
      confidence_score: 0.71,
      latest_signal_summary: "Moderate risk and rising productivity anomaly suggest confirmatory sampling.",
    },
    {
      site_id: "farm-sd-03",
      site_name: "South Bay Nursery Intake",
      lat: 32.56,
      lon: -117.18,
      recommendation: "warn_buyers",
      risk_score: 0.81,
      harvest_window_label: "Pause intake and notify buyers",
      confidence_score: 0.79,
      latest_signal_summary: "Conditions resemble prior high-risk episodes with potential buyer-impact implications.",
    },
  ],
  alerts: mockAlerts,
}

export const mockGrowerSiteDetails: Record<string, GrowerSiteDetail> = {
  "farm-sd-01": {
    site_id: "farm-sd-01",
    site_name: "Coronado Kelp & Shell Site",
    recommendation: "harvest",
    confidence_score: 0.83,
    harvest_window_label: "Harvest within 18 hours",
    forecast: [
      { t: iso(0), value: 0.28 },
      { t: iso(12), value: 0.31 },
      { t: iso(24), value: 0.42 },
      { t: iso(48), value: 0.46 },
    ],
    top_factors: [
      { name: "short_harvest_window", direction: "down", magnitude: 0.78, description: "Conditions favor immediate harvest relative to later windows." },
      { name: "low_toxin_propensity", direction: "down", magnitude: 0.73, description: "Current risk remains low but begins to rise after 24 hours." },
    ],
    advisories: mockAlerts,
    sampling_priorities: [
      { label: "Post-harvest verification", priority: "medium", reason: "Confidence is good, but documentation strengthens buyer trust." },
      { label: "Surface toxin sample", priority: "low", reason: "Current model risk is low." },
    ],
    buyer_warning_recommended: false,
    recommended_action_text: "Harvest this site now. Delay reduces margin as risk begins climbing tomorrow.",
    evidence: [
      "Lowest current toxin propensity among active sites.",
      "Favorable short harvest window closes as productivity anomaly spreads south.",
    ],
  },
  "farm-sd-03": {
    site_id: "farm-sd-03",
    site_name: "South Bay Nursery Intake",
    recommendation: "warn_buyers",
    confidence_score: 0.79,
    harvest_window_label: "Pause intake and notify buyers",
    forecast: [
      { t: iso(0), value: 0.81 },
      { t: iso(12), value: 0.85 },
      { t: iso(24), value: 0.88 },
      { t: iso(48), value: 0.76 },
    ],
    top_factors: [
      { name: "intake_risk", direction: "up", magnitude: 0.88, description: "Current nearshore signal is strongly elevated for intake-sensitive operations." },
      { name: "buyer_impact_risk", direction: "up", magnitude: 0.71, description: "Recommend proactive communication to preserve trust and avoid downstream surprises." },
    ],
    advisories: mockAlerts,
    sampling_priorities: [
      { label: "Immediate toxin sample", priority: "high", reason: "Model risk exceeds warning threshold." },
      { label: "Buyer communication packet", priority: "high", reason: "Operational and reputational risk are both elevated." },
    ],
    buyer_warning_recommended: true,
    recommended_action_text: "Pause intake and notify buyers. Conditions are too similar to prior disruptive risk episodes.",
    evidence: [
      "Projected risk remains high over the next 24 hours.",
      "Multiple transport and biology signals align with prior damaging periods.",
    ],
  },
}

export function buildMockMap(shell: "fleet" | "grower", wrapper: WrapperType, horizon: string): MapRiskResponse {
  if (shell === "grower") {
    return {
      generated_at_utc: iso(),
      shell,
      wrapper,
      horizon,
      locations: mockGrowerDashboard.sites.map((site) => ({
        location_id: site.site_id,
        name: site.site_name,
        lat: site.lat,
        lon: site.lon,
        risk_score: site.risk_score,
        risk_bucket: site.risk_score > 0.75 ? "high" : site.risk_score > 0.45 ? "elevated" : "moderate",
        uncertainty_score: 1 - site.confidence_score,
        recommendation: site.recommendation,
      })),
    }
  }

  return {
    generated_at_utc: iso(),
    shell,
    wrapper,
    horizon,
    locations: mockFleetDashboard.map_points,
  }
}

export const mockScienceSummary: ScienceSummary = {
  datasets: [
    { key: "calcofi", name: "CalCOFI", role: "Historical ecological + chemistry backbone", status: "connected", granularity: "Cruise/station" },
    { key: "cce", name: "CCE Moorings", role: "High-frequency near-real-time event signals", status: "connected", granularity: "Mooring/time series" },
    { key: "argo", name: "EasyOneArgo", role: "Regional offshore context", status: "connected", granularity: "Profile" },
    { key: "cdip", name: "CDIP", role: "Wave / transport context", status: "mocked", granularity: "Station/time series" },
    { key: "beach", name: "Beach advisories", role: "Beach safety supervision", status: "mocked", granularity: "Beach/event" },
    { key: "biotoxin", name: "Marine biotoxin signals", role: "Fishing/grower toxicity supervision", status: "mocked", granularity: "Zone/sample" },
  ],
  model_runs: [
    { run_id: "run-101", name: "multitask-tabular-v1", status: "success", target: "fishing risk head", score_label: "PR-AUC", score_value: "0.71", notes: "Best current fused baseline." },
    { run_id: "run-102", name: "beach-advisory-head", status: "running", target: "beach safety", score_label: "Lead time", score_value: "+18h", notes: "Optimizing recall-heavy threshold." },
    { run_id: "run-103", name: "grower-harvest-head", status: "success", target: "harvest decision head", score_label: "Decision uplift", score_value: "+12%", notes: "Mocked decision-uplift metric for demo." },
  ],
  architecture_cards: [
    { title: "Bronze → Silver → Gold", value: "Active", delta: "Lakehouse path", tone: "good" },
    { title: "Shared risk engine", value: "4 heads", delta: "Beach/Fishing/Surf/Grower", tone: "neutral" },
    { title: "Serving path", value: "Django + AWS", delta: "Databricks-backed", tone: "good" },
    { title: "Demo mode", value: "Mock-first", delta: "Real-ready contracts", tone: "warn" },
  ],
}

export const mockPipelineStatus: PipelineStatus = {
  generated_at_utc: iso(),
  bronze: [
    { source: "CalCOFI ingest", status: "ready", last_updated: iso(-6), note: "Historical backbone landed." },
    { source: "CCE moorings", status: "running", last_updated: iso(-1), note: "Refreshing latest high-frequency signals." },
  ],
  silver: [
    { source: "Canonical coastal state", status: "running", last_updated: iso(-1), note: "Time/depth harmonization in progress." },
    { source: "Label harmonization", status: "pending", last_updated: iso(-3), note: "Waiting on latest advisory blend." },
  ],
  gold: [
    { source: "Fleet recommendation artifacts", status: "ready", last_updated: iso(-2), note: "Cached demo export available." },
    { source: "Grower decision artifacts", status: "ready", last_updated: iso(-2), note: "Scaffold export available." },
  ],
  serving: [
    { source: "Django cache sync", status: "ready", last_updated: iso(-1), note: "API payloads current." },
    { source: "Frontend static bundle", status: "ready", last_updated: iso(-1), note: "Mock mode enabled." },
  ],
}
