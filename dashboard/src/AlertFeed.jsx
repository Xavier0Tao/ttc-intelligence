import { useMemo } from 'react';

/** Alerts carry `timestamp` in seconds; ripples carry `detected_at` in ms. */
function sortTime(item) {
  if (item.detected_at) return item.detected_at;
  return (item.timestamp || 0) * 1000;
}

export default function AlertFeed({ alerts, ripples }) {
  const items = useMemo(() => {
    const list = [
      ...Array.from(alerts.values()).map((a) => ({ kind: 'alert', item: a })),
      ...Array.from(ripples.values()).map((r) => ({ kind: 'ripple', item: r })),
    ];
    list.sort((a, b) => sortTime(b.item) - sortTime(a.item));
    return list;
  }, [alerts, ripples]);

  return (
    <div className="panel alert-feed">
      <h2>Alerts <span className="hint">({alerts.size} alerts, {ripples.size} ripples)</span></h2>
      <ul>
        {items.map(({ kind, item }) => (
          kind === 'alert' ? (
            <li key={`a-${item.alert_id}`} className="alert-item">
              <span className={`badge effect-${item.effect === 'NO_SERVICE' ? 'severe' : 'info'}`}>
                {item.effect?.replaceAll('_', ' ')}
              </span>
              <span className="alert-text">{item.header_text}</span>
            </li>
          ) : (
            <li key={`r-${item.ripple_id}`} className="ripple-item">
              <span className="badge badge-ripple">⤷ RIPPLE</span>
              <span className="alert-text">
                Route <b>{item.feeder_route_id}</b> near <b>{item.affected_station}</b>:{' '}
                {item.predicted_impact?.replaceAll('_', ' ').toLowerCase()}
                <span className="dim"> — cascaded from {item.source_alert_id}</span>
              </span>
            </li>
          )
        ))}
        {items.length === 0 && <li className="dim">no active alerts</li>}
      </ul>
    </div>
  );
}
