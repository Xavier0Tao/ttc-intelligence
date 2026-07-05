import { memo, useEffect, useRef, useState } from 'react';
import { CircleMarker, MapContainer, Marker, TileLayer, Tooltip, useMapEvents } from 'react-leaflet';
import { iconFor, vehicleKindForRouteType } from './vehicleIcons';

const TORONTO_CENTER = [43.7, -79.4];
const DEFAULT_ZOOM = 12;

/**
 * Leaflet animates zoom with the same transform the smooth-move CSS
 * transition targets; disabling the transition during zoom prevents markers
 * from visibly drifting to their new projected positions.
 */
function ZoomTransitionGuard() {
  useMapEvents({
    zoomstart: (e) => e.target.getContainer().classList.add('map-zooming'),
    zoomend: (e) => e.target.getContainer().classList.remove('map-zooming'),
  });
  return null;
}

const VehicleMarker = memo(function VehicleMarker({ vehicle, routeType }) {
  const kind = vehicleKindForRouteType(routeType);
  return (
    <Marker
      position={[vehicle.latitude, vehicle.longitude]}
      icon={iconFor(kind, vehicle.crowding_level)}
    >
      <Tooltip direction="top" offset={[0, -12]}>
        <b>Route {vehicle.route_id || '?'}</b> · {kind} {vehicle.vehicle_id}
        <br />
        crowding: {vehicle.crowding_level || 'UNKNOWN'}
      </Tooltip>
    </Marker>
  );
}, (prev, next) =>
  prev.vehicle.latitude === next.vehicle.latitude
  && prev.vehicle.longitude === next.vehicle.longitude
  && prev.vehicle.crowding_level === next.vehicle.crowding_level
  && prev.routeType === next.routeType);

/** Temporary overlay of a hovered route's stops, cached per route. */
function RouteStopsOverlay({ routeId }) {
  const cacheRef = useRef(new Map());
  const [stops, setStops] = useState([]);

  useEffect(() => {
    if (!routeId) {
      setStops([]);
      return;
    }
    const cached = cacheRef.current.get(routeId);
    if (cached) {
      setStops(cached);
      return;
    }
    let cancelled = false;
    fetch(`/api/routes/${encodeURIComponent(routeId)}/stops`)
      .then((r) => r.json())
      .then((data) => {
        cacheRef.current.set(routeId, data);
        if (!cancelled) setStops(data);
      })
      .catch(() => {
        if (!cancelled) setStops([]);
      });
    return () => {
      cancelled = true;
    };
  }, [routeId]);

  if (!routeId) return null;
  return stops.map((s) => (
    <CircleMarker
      key={s.stop_id}
      center={[s.latitude, s.longitude]}
      radius={4}
      pathOptions={{ color: '#444', fillColor: '#777', fillOpacity: 0.9, weight: 1 }}
    >
      <Tooltip direction="top">{s.stop_name}</Tooltip>
    </CircleMarker>
  ));
}

export default function MapView({ vehicles, routeTypes, hoveredRoute }) {
  const markers = [];
  vehicles.forEach((v) => {
    if (typeof v.latitude !== 'number' || typeof v.longitude !== 'number') return;
    if (v.latitude === 0 && v.longitude === 0) return;
    markers.push(
      <VehicleMarker
        key={v.vehicle_id}
        vehicle={v}
        routeType={routeTypes.get(v.route_id)}
      />,
    );
  });

  return (
    <MapContainer center={TORONTO_CENTER} zoom={DEFAULT_ZOOM} className="map-root">
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <ZoomTransitionGuard />
      {markers}
      <RouteStopsOverlay routeId={hoveredRoute} />
    </MapContainer>
  );
}
