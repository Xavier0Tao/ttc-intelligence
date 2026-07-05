import { useEffect, useState } from 'react';
import AlertFeed from './AlertFeed';
import ConnectionStatus from './ConnectionStatus';
import DelayPanel from './DelayPanel';
import MapView from './MapView';
import useLiveFeed from './useLiveFeed';

export default function App() {
  const { status, vehicles, delays, alerts, ripples } = useLiveFeed();
  const [routeTypes, setRouteTypes] = useState(() => new Map());
  const [hoveredRoute, setHoveredRoute] = useState(null);

  // Route metadata (short name -> route_type) drives per-vehicle icon choice.
  useEffect(() => {
    fetch('/api/routes')
      .then((r) => r.json())
      .then((routes) => {
        const m = new Map();
        routes.forEach((r) => m.set(r.route_short_name, r.route_type));
        setRouteTypes(m);
      })
      .catch(() => {});
  }, []);

  return (
    <div className="app">
      <div className="map-pane">
        <MapView vehicles={vehicles} routeTypes={routeTypes} hoveredRoute={hoveredRoute} />
        <ConnectionStatus status={status} />
        <div className="map-title">
          TTC Intelligence <span className="dim">· {vehicles.size} vehicles live</span>
        </div>
      </div>
      <div className="side-pane">
        <DelayPanel delays={delays} onHoverRoute={setHoveredRoute} />
        <AlertFeed alerts={alerts} ripples={ripples} />
      </div>
    </div>
  );
}
