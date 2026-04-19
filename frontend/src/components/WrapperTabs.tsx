import type { ProductShell, WrapperType } from "../types"

const GROWER_WRAPPERS: WrapperType[] = ["aquaculture", "beach", "ecosystem"]
const FLEET_WRAPPERS: WrapperType[] = ["fishing", "beach", "ecosystem"]

export function WrapperTabs({
  shell,
  value,
  onChange,
}: {
  shell: ProductShell
  value: WrapperType
  onChange: (value: WrapperType) => void
}) {
  const wrappers = shell === "grower" ? GROWER_WRAPPERS : FLEET_WRAPPERS

  return (
    <div className="wrapper-tabs">
      {wrappers.map((wrapper) => (
        <button key={wrapper} className={wrapper === value ? "active" : ""} onClick={() => onChange(wrapper)}>
          {wrapper}
        </button>
      ))}
    </div>
  )
}