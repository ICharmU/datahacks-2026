import type { DataMode, ProductShell, ShellSwitcherOption } from "./types"

export const APP_CONFIG = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL || "/api/v1",
  initialDataMode: (String(import.meta.env.VITE_USE_MOCK_DATA).toLowerCase() === "true" ? "mock" : "real") as DataMode,
  defaultShell: "fleet" as ProductShell,
}

export const SHELL_OPTIONS: ShellSwitcherOption[] = [
  {
    key: "fleet",
    label: "Fleet Shell",
    subtitle: "Rank fishing zones, target species, and trip opportunity under risk constraints.",
  },
  {
    key: "grower",
    label: "Grower Shell",
    subtitle: "Decide when to harvest, delay, sample, or warn buyers.",
  },
]
