import { useEffect, useMemo, useState } from "react"
import { APP_CONFIG } from "../config"
import type { DataMode, ProductShell } from "../types"

const MODE_KEY = "toxictide:data-mode"
const SHELL_KEY = "toxictide:product-shell"

export function useAppMode() {
  const [dataMode, setDataMode] = useState<DataMode>(APP_CONFIG.initialDataMode)
  const [shell, setShell] = useState<ProductShell>(APP_CONFIG.defaultShell)

  useEffect(() => {
    const savedMode = window.localStorage.getItem(MODE_KEY) as DataMode | null
    const savedShell = window.localStorage.getItem(SHELL_KEY) as ProductShell | null
    if (savedMode === "mock" || savedMode === "real") setDataMode(savedMode)
    if (savedShell === "fleet" || savedShell === "grower") setShell(savedShell)
  }, [])

  useEffect(() => {
    window.localStorage.setItem(MODE_KEY, dataMode)
  }, [dataMode])

  useEffect(() => {
    window.localStorage.setItem(SHELL_KEY, shell)
  }, [shell])

  const apiModeLabel = useMemo(() => (dataMode === "mock" ? "Mock data" : "Live backend"), [dataMode])

  return {
    dataMode,
    setDataMode,
    shell,
    setShell,
    apiModeLabel,
  }
}
