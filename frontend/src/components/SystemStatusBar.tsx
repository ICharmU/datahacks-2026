export function SystemStatusBar({ modeLabel }: { modeLabel: string }) {
  return (
    <div className="system-bar">
      <span>Frontend ready for Databricks-backed contracts</span>
      <span>Mode: {modeLabel}</span>
    </div>
  )
}
