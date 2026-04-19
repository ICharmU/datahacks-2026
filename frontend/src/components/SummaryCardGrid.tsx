import type { ShellSummaryCard } from "../types"

export function SummaryCardGrid({ cards }: { cards: ShellSummaryCard[] }) {
  return (
    <div className="summary-grid">
      {cards.map((card) => (
        <div key={card.title} className={`summary-card ${card.tone}`}>
          <div className="summary-card__title">{card.title}</div>
          <div className="summary-card__value">{card.value}</div>
          {card.delta && <div className="summary-card__delta">{card.delta}</div>}
        </div>
      ))}
    </div>
  )
}
