export type WrapperType = "beach" | "fishing" | "surf" | "ecosystem";

export interface MapRiskPoint {
  location_id: string;
  name: string;
  lat: number;
  lon: number;
  risk_score: number;
  risk_bucket: string;
  uncertainty_score?: number;
}

export interface MapRiskResponse {
  generated_at_utc: string;
  wrapper: WrapperType;
  horizon: string;
  locations: MapRiskPoint[];
}

export interface FactorContribution {
  name: string;
  direction: "up" | "down";
  magnitude: number;
}

export interface ForecastPoint {
  t: string;
  risk_score: number;
}

export interface BeachRiskDetail {
  location_id: string;
  name: string;
  wrapper: WrapperType;
  forecast: ForecastPoint[];
  risk_bucket: string;
  uncertainty_score?: number;
  top_factors: FactorContribution[];
  recommended_action: string;
  evidence: string[];
}