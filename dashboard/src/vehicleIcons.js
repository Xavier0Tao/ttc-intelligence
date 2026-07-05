import L from 'leaflet';

// Generic silhouette glyphs (not TTC-branded assets), filled TTC red.
const TTC_RED = '#DA291C';

const GLYPHS = {
  // Front-facing bus: body, windshield, wheels
  bus: `<svg viewBox="0 0 24 24" width="14" height="14" fill="${TTC_RED}"><path d="M5 3h14a2 2 0 0 1 2 2v11a2 2 0 0 1-1 1.73V20a1 1 0 0 1-1 1h-1a1 1 0 0 1-1-1v-1H7v1a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-2.27A2 2 0 0 1 3 16V5a2 2 0 0 1 2-2zm0 3v5h14V6H5zm2.5 9.5A1.5 1.5 0 1 0 7.5 13a1.5 1.5 0 0 0 0 3zm9 0a1.5 1.5 0 1 0 0-3 1.5 1.5 0 0 0 0 3z"/></svg>`,
  // Side-view streetcar: long body with pantograph
  streetcar: `<svg viewBox="0 0 24 24" width="15" height="15" fill="${TTC_RED}"><path d="M11 2h2v2h5v2H6V4h5V2zM5 7h14a1.5 1.5 0 0 1 1.5 1.5v8A1.5 1.5 0 0 1 19 18h-.6l1.1 2h-2.2l-1.1-2H7.8l-1.1 2H4.5l1.1-2H5a1.5 1.5 0 0 1-1.5-1.5v-8A1.5 1.5 0 0 1 5 7zm.5 2v4h5V9h-5zm8 0v4h5V9h-5z"/></svg>`,
  // Front-facing subway train: rounded body, single windshield
  subway: `<svg viewBox="0 0 24 24" width="14" height="14" fill="${TTC_RED}"><path d="M12 2c4.4 0 8 .9 8 4v10.5a2.5 2.5 0 0 1-1.6 2.33L20 21h-2.4l-1.2-2H7.6l-1.2 2H4l1.6-2.17A2.5 2.5 0 0 1 4 16.5V6c0-3.1 3.6-4 8-4zM6 7v5h12V7H6zm2 9.5A1.5 1.5 0 1 0 8 13.5a1.5 1.5 0 0 0 0 3zm8 0a1.5 1.5 0 1 0 0-3 1.5 1.5 0 0 0 0 3z"/></svg>`,
};

const RING_COLORS = {
  NORMAL: '#2e7d32',
  LIKELY_CROWDED: '#c62828',
  LIKELY_EMPTY: '#1565c0',
  UNKNOWN: '#9aa0a6',
};

export function vehicleKindForRouteType(routeType) {
  if (routeType === 1) return 'subway';
  if (routeType === 0) return 'streetcar';
  return 'bus';
}

// divIcon instances are cached per (kind, crowding) — there are at most 12
// distinct icons, and reusing them avoids re-creating DOM on every update.
const iconCache = new Map();

export function iconFor(kind, crowdingLevel) {
  const level = RING_COLORS[crowdingLevel] ? crowdingLevel : 'UNKNOWN';
  const key = `${kind}:${level}`;
  let icon = iconCache.get(key);
  if (!icon) {
    icon = L.divIcon({
      className: 'veh-wrap',
      html: `<div class="veh-icon" style="--ring:${RING_COLORS[level]}">${GLYPHS[kind]}</div>`,
      iconSize: [26, 26],
      iconAnchor: [13, 13],
    });
    iconCache.set(key, icon);
  }
  return icon;
}
