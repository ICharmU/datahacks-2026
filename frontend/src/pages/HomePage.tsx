import { Link } from "react-router-dom"
import type { ProductShell } from "../types"

export function HomePage({ shell }: { shell: ProductShell }) {
  return (
    <div className="page-stack">
      <section className="hero-card">
        <div className="eyebrow">Environment · climate · coastal operations</div>
        <h1>AquaYield helps growers act before nearshore coastal risk becomes costly.</h1>
        <p>
          We fuse coastal chemistry, contamination proxies, tides, and multi-source ocean-state signals into aquaculture watchlists,
          beach-risk surfaces, and evidence-backed explanations.
        </p>
        <div className="hero-actions">
          <Link to={shell === "fleet" ? "/fleet" : "/grower"} className="primary-link">Open active shell</Link>
          <Link to="/map" className="secondary-link">Open map</Link>
          <Link to="/science" className="secondary-link">See science layer</Link>
        </div>
      </section>

      <section className="two-col-grid">
        <div className="panel">
          <div className="panel__title">Grower shell</div>
          <p>Decide when to harvest, delay, sample, or warn buyers using a nearshore risk engine built for aquaculture operations.</p>
          <Link to="/grower">Go to grower dashboard</Link>
        </div>
        <div className="panel">
          <div className="panel__title">Beach companion shell</div>
          <p>Demonstrate the same coastal intelligence backbone on public-facing beach safety and risk visualization.</p>
          <Link to="/map">Open map</Link>
        </div>
      </section>
    </div>
  )
}