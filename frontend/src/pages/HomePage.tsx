import { Link } from "react-router-dom"
import type { ProductShell } from "../types"

export function HomePage({ shell }: { shell: ProductShell }) {
  return (
    <div className="page-stack">
      <section className="hero-card">
        <div className="eyebrow">Environment · climate · coastal operations</div>
        <h1>Know where coastal risk is rising before operators pay the price.</h1>
        <p>
          BlueYield fuses coastal observations, bloom and toxin signals, ecosystem stress, and operational constraints into decision-ready products for fleets and growers.
        </p>
        <div className="hero-actions">
          <Link to={shell === "fleet" ? "/fleet" : "/grower"} className="primary-link">Open active shell</Link>
          <Link to="/science" className="secondary-link">See science layer</Link>
        </div>
      </section>

      <section className="two-col-grid">
        <div className="panel">
          <div className="panel__title">Fleet shell</div>
          <p>Show ranked fishing zones, target-species opportunity, toxin and closure risk, and trip economics.</p>
          <Link to="/fleet">Go to fleet dashboard</Link>
        </div>
        <div className="panel">
          <div className="panel__title">Grower shell</div>
          <p>Show which sites to harvest, delay, sample, or escalate to buyers under nearshore bloom risk.</p>
          <Link to="/grower">Go to grower dashboard</Link>
        </div>
      </section>
    </div>
  )
}
