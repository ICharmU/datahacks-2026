import type { DataMode, ProductShell, ShellSwitcherOption } from "./types"

export const APP_CONFIG = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL || "/api/v1",
  initialDataMode: (String(import.meta.env.VITE_USE_MOCK_DATA).toLowerCase() === "true" ? "mock" : "real") as DataMode,
  defaultShell: "grower" as ProductShell,
}

export const SHELL_OPTIONS: ShellSwitcherOption[] = [
  {
    key: "grower",
    label: "Grower Shell",
    subtitle: "Harvest, delay, sample, or warn buyers using nearshore aquaculture risk intelligence.",
  },
  {
    key: "fleet",
    label: "Fleet Shell",
    subtitle: "Secondary shell kept alive for demo completeness and future expansion.",
  },
]