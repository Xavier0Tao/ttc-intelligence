import { useCallback, useEffect, useRef, useState } from 'react';

// Kafka poll cycles arrive as bursts of individual WS messages (one per
// vehicle); rendering per-message would thrash React. Updates accumulate in a
// ref and flush to state on a short interval instead.
const FLUSH_INTERVAL_MS = 500;
// Client-side prune of vehicles this page hasn't heard from in a while (the
// server's Redis TTL handles the snapshot; this handles a long-lived page).
// Staleness is measured from the client's own receipt time (_seenAtMs),
// NOT the feed's embedded vehicle.timestamp: that's the vehicle's last GPS
// fix, which routinely lags real time by minutes even for actively-tracked
// vehicles, and comparing it against wall-clock time evicted almost
// everything within one flush cycle.
const STALE_VEHICLE_MS = 180_000;
const BACKOFF_START_MS = 1000;
const BACKOFF_CAP_MS = 30000;

function emptyPending() {
  return {
    vehicles: new Map(),
    crowding: new Map(),
    delays: new Map(),
    alerts: new Map(),
    ripples: new Map(),
  };
}

// Debug aid: per-type counts of received WS messages, inspectable from the
// browser console as window.__feedStats.
const feedStats = {};
if (typeof window !== 'undefined') {
  window.__feedStats = feedStats;
}

export default function useLiveFeed() {
  const [status, setStatus] = useState('reconnecting');
  const [vehicles, setVehicles] = useState(() => new Map());
  const [delays, setDelays] = useState(() => new Map());
  const [alerts, setAlerts] = useState(() => new Map());
  const [ripples, setRipples] = useState(() => new Map());

  const wsRef = useRef(null);
  const backoffRef = useRef(BACKOFF_START_MS);
  const pendingRef = useRef(emptyPending());
  const disposedRef = useRef(false);

  const applySnapshot = useCallback((snap) => {
    const toMap = (items, key, stamp) => {
      const m = new Map();
      (items || []).forEach((x) => x && x[key] && m.set(x[key], stamp ? { ...x, _seenAtMs: Date.now() } : x));
      return m;
    };
    pendingRef.current = emptyPending();
    setVehicles(toMap(snap.vehicles, 'vehicle_id', true));
    setDelays(toMap(snap.delays, 'route_id'));
    setAlerts(toMap(snap.alerts, 'alert_id'));
    setRipples(toMap(snap.ripples, 'ripple_id'));
  }, []);

  useEffect(() => {
    disposedRef.current = false;

    function connect() {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(`${proto}//${window.location.host}/ws/live`);
      wsRef.current = ws;

      ws.onopen = () => {
        setStatus('connected');
        backoffRef.current = BACKOFF_START_MS;
        // The WS sends its own snapshot on connect, but if the gateway
        // restarted while we were reconnecting, an explicit REST resync
        // guards against any state raced in between.
        fetch('/api/snapshot')
          .then((r) => r.json())
          .then(applySnapshot)
          .catch(() => {});
      };

      ws.onmessage = (event) => {
        let msg;
        try {
          msg = JSON.parse(event.data);
        } catch {
          return;
        }
        feedStats[msg.type] = (feedStats[msg.type] || 0) + 1;
        const p = pendingRef.current;
        const d = msg.data;
        switch (msg.type) {
          case 'snapshot':
            applySnapshot(msg);
            break;
          case 'vehicle_update':
            if (d?.vehicle_id) p.vehicles.set(d.vehicle_id, d);
            break;
          case 'crowding_update':
            if (d?.vehicle_id) p.crowding.set(d.vehicle_id, d);
            break;
          case 'delay_update':
            if (d?.route_id) p.delays.set(d.route_id, d);
            break;
          case 'alert_update':
            if (d?.alert_id) p.alerts.set(d.alert_id, d);
            break;
          case 'ripple_update':
            if (d?.ripple_id) p.ripples.set(d.ripple_id, d);
            break;
          default:
            break;
        }
      };

      ws.onclose = () => {
        if (disposedRef.current) return;
        setStatus('reconnecting');
        const delay = backoffRef.current;
        backoffRef.current = Math.min(delay * 2, BACKOFF_CAP_MS);
        setTimeout(() => {
          if (!disposedRef.current) connect();
        }, delay);
      };

      ws.onerror = () => {
        try {
          ws.close();
        } catch {
          /* already closed */
        }
      };
    }

    connect();

    const flusher = setInterval(() => {
      // Detach the accumulated batch FIRST: React batches setState from
      // intervals, so the updater closures below run later, at render time.
      // If we mutated/reset the pending maps after calling setState (the
      // original bug), the updaters would see already-emptied maps and every
      // live update would be silently discarded.
      const batch = pendingRef.current;
      pendingRef.current = emptyPending();

      // Vehicles flush runs every tick even with an empty batch so the
      // staleness prune keeps working when the feed goes quiet.
      setVehicles((prev) => {
        const next = new Map(prev);
        const now = Date.now();
        batch.vehicles.forEach((v, id) => next.set(id, { ...next.get(id), ...v, _seenAtMs: now }));
        batch.crowding.forEach((c, id) => {
          const cur = next.get(id);
          if (cur) next.set(id, { ...cur, crowding_level: c.crowding_level });
        });
        const cutoff = now - STALE_VEHICLE_MS;
        next.forEach((v, id) => {
          if (v._seenAtMs && v._seenAtMs < cutoff) next.delete(id);
        });
        return next;
      });
      if (batch.delays.size) {
        setDelays((prev) => {
          const next = new Map(prev);
          batch.delays.forEach((v, id) => next.set(id, v));
          return next;
        });
      }
      if (batch.alerts.size) {
        setAlerts((prev) => {
          const next = new Map(prev);
          batch.alerts.forEach((v, id) => next.set(id, v));
          return next;
        });
      }
      if (batch.ripples.size) {
        setRipples((prev) => {
          const next = new Map(prev);
          batch.ripples.forEach((v, id) => next.set(id, v));
          return next;
        });
      }
    }, FLUSH_INTERVAL_MS);

    return () => {
      disposedRef.current = true;
      clearInterval(flusher);
      wsRef.current?.close();
    };
  }, [applySnapshot]);

  return { status, vehicles, delays, alerts, ripples };
}
