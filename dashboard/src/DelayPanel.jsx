import { useMemo, useState } from 'react';

function scoreClass(score) {
  if (score >= 2) return 'score-bad';
  if (score <= -2) return 'score-ahead';
  return 'score-ok';
}

export default function DelayPanel({ delays, onHoverRoute }) {
  const [sortKey, setSortKey] = useState('delay_score');
  const [sortDir, setSortDir] = useState(-1);

  const rows = useMemo(() => {
    const list = Array.from(delays.values());
    list.sort((a, b) => {
      const va = sortKey === 'route_id' ? a.route_id : a.delay_score;
      const vb = sortKey === 'route_id' ? b.route_id : b.delay_score;
      if (sortKey === 'route_id') {
        return sortDir * String(va).localeCompare(String(vb), undefined, { numeric: true });
      }
      return sortDir * (va - vb);
    });
    return list;
  }, [delays, sortKey, sortDir]);

  const toggleSort = (key) => {
    if (key === sortKey) {
      setSortDir(-sortDir);
    } else {
      setSortKey(key);
      setSortDir(key === 'route_id' ? 1 : -1);
    }
  };

  const arrow = (key) => (sortKey === key ? (sortDir > 0 ? ' ▲' : ' ▼') : '');

  return (
    <div className="panel delay-panel">
      <h2>Route delays <span className="hint">(hover a row to see stops)</span></h2>
      <table>
        <thead>
          <tr>
            <th onClick={() => toggleSort('route_id')}>Route{arrow('route_id')}</th>
            <th onClick={() => toggleSort('delay_score')}>Delay score{arrow('delay_score')}</th>
            <th>Actual / scheduled (min)</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((d) => (
            <tr
              key={d.route_id}
              onMouseEnter={() => onHoverRoute(d.route_id)}
              onMouseLeave={() => onHoverRoute(null)}
            >
              <td className="route-cell">{d.route_id}</td>
              <td className={scoreClass(d.delay_score)}>
                {d.delay_score > 0 ? '+' : ''}{d.delay_score?.toFixed(2)}
              </td>
              <td className="dim">
                {d.actual_headway_minutes?.toFixed(1)} / {d.scheduled_headway_minutes?.toFixed(1)}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr><td colSpan="3" className="dim">waiting for the first 5-minute window…</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
