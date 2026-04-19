export function SystemStatusBar({ modeLabel }: { modeLabel: string }) {
  return (
    <div className="system-bar">
      <span>AquaYield: Databricks → S3 serving → Django → React</span>
      <span>Mode: {modeLabel}</span>
    </div>
  )
}