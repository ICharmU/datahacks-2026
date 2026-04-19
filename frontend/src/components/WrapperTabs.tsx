import type { WrapperType } from "../types"

const WRAPPERS: WrapperType[] = ["beach", "fishing", "surf", "ecosystem"]

export function WrapperTabs({ value, onChange }: { value: WrapperType; onChange: (value: WrapperType) => void }) {
  return (
    <div className="wrapper-tabs">
      {WRAPPERS.map((wrapper) => (
        <button key={wrapper} className={wrapper === value ? "active" : ""} onClick={() => onChange(wrapper)}>
          {wrapper}
        </button>
      ))}
    </div>
  )
}
