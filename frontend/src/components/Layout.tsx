import type { ReactNode } from "react"
import { Link, NavLink } from "react-router-dom"
import type { DataMode, ProductShell } from "../types"
import { SHELL_OPTIONS } from "../config"

function ShellToggle({ shell, onChange }: { shell: ProductShell; onChange: (shell: ProductShell) => void }) {
  return (
    <div className="shell-toggle">
      {SHELL_OPTIONS.map((option) => (
        <button
          key={option.key}
          className={`shell-toggle__button ${shell === option.key ? "active" : ""}`}
          onClick={() => onChange(option.key)}
        >
          <span>{option.label}</span>
          <small>{option.subtitle}</small>
        </button>
      ))}
    </div>
  )
}

function DataModeToggle({ mode, onChange }: { mode: DataMode; onChange: (mode: DataMode) => void }) {
  return (
    <div className="mode-toggle">
      <button className={mode === "mock" ? "active" : ""} onClick={() => onChange("mock")}>Mock</button>
      <button className={mode === "real" ? "active" : ""} onClick={() => onChange("real")}>Live</button>
    </div>
  )
}

export function Layout({
  children,
  shell,
  onShellChange,
  dataMode,
  onDataModeChange,
}: {
  children: ReactNode
  shell: ProductShell
  onShellChange: (shell: ProductShell) => void
  dataMode: DataMode
  onDataModeChange: (mode: DataMode) => void
}) {
  return (
    <div className="app-shell">
      <header className="app-header">
        <div>
          <Link to="/" className="brand">BlueYield</Link>
          <p className="brand-subtitle">Climate-smart coastal decision platform for fleets and growers.</p>
        </div>
        <DataModeToggle mode={dataMode} onChange={onDataModeChange} />
      </header>

      <ShellToggle shell={shell} onChange={onShellChange} />

      <nav className="app-nav">
        <NavLink to="/">Overview</NavLink>
        <NavLink to="/fleet">Fleet</NavLink>
        <NavLink to="/grower">Grower</NavLink>
        <NavLink to="/map">Map</NavLink>
        <NavLink to="/science">Science</NavLink>
        <NavLink to="/pipeline">Pipeline</NavLink>
      </nav>

      <main>{children}</main>
    </div>
  )
}
