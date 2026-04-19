import type { DataMode } from "../types"
import { mockApi } from "./mockApi"
import { realApi } from "./realApi"

export function getApi(mode: DataMode) {
  return mode === "mock" ? mockApi : realApi
}
