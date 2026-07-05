"""One-shot loader for TTC static GTFS data.

Downloads the City of Toronto's merged GTFS zip (all modes: subway,
streetcar, bus), computes scheduled headway per route per hour of day, maps
which surface routes feed each subway station, and loads the results into
TimescaleDB. Safe to re-run: every table is fully refreshed.
"""

import csv
import io
import math
import os
import sys
import zipfile
from collections import defaultdict
from datetime import date

import psycopg2
import psycopg2.extras
import requests

# "Merged GTFS - TTC Routes and Schedules" on open.toronto.ca — covers all
# modes, unlike the surface-only SurfaceGTFS.zip this project used originally.
GTFS_URL = os.environ.get(
    "GTFS_URL",
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/b811ead4-6eaf-4adb-8408-d389fb5a069c/resource/c920e221-7a1c-488b-8c5b-6d8cd4e85eaf/download/completegtfs.zip",
)

# Surface stops within this distance of a subway station are considered to
# feed it. Tunable: large interchange stations (Kennedy, Finch, Scarborough
# Centre) have bus bays spread further out and may warrant a larger radius.
FEEDER_RADIUS_M = 300

EARTH_RADIUS_M = 6_371_000

SUBWAY_ROUTE_TYPE = 1


def haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle distance in meters between two lat/lon points."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def db_connect():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "ttc_intelligence"),
        user=os.environ.get("DB_USER", "ttc"),
        password=os.environ.get("DB_PASSWORD", "ttc"),
    )


def download_gtfs(url):
    print(f"downloading static GTFS from {url} ...", flush=True)
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    print(f"downloaded {len(resp.content) / 1_048_576:.1f} MB", flush=True)
    return zipfile.ZipFile(io.BytesIO(resp.content))


def read_csv(zf, name):
    with zf.open(name) as f:
        # utf-8-sig strips the BOM that GTFS files commonly start with
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))


def parse_hour(gtfs_time):
    """GTFS times can exceed 24:00:00 for trips running past midnight."""
    hour = int(gtfs_time.split(":", 1)[0])
    return hour % 24


def select_weekday_services(zf):
    """Pick service_ids representing a single typical weekday.

    trips.txt spans every service pattern (weekday/Saturday/Sunday, often
    across multiple board periods), so counting all trips inflates
    trips-per-hour several-fold and produces absurd headways. We keep only
    services active on Wednesdays, preferring the board period that contains
    today; if today falls outside every period, we use the most recent one.
    Returns None if calendar.txt is missing (caller falls back to all trips).
    """
    try:
        rows = list(read_csv(zf, "calendar.txt"))
    except KeyError:
        return None

    weekday_rows = [r for r in rows if r.get("wednesday", "0").strip() == "1"]
    if not weekday_rows:
        return None

    # Feeds often contain several overlapping board periods; keeping more than
    # one double-counts every trip. Group services by their validity window
    # and keep exactly one window — the current one, else the most recent.
    groups = defaultdict(set)
    for r in weekday_rows:
        groups[(r.get("start_date", ""), r.get("end_date", ""))].add(r["service_id"])

    today = date.today().strftime("%Y%m%d")
    containing = [w for w in groups if w[0] <= today <= w[1]]
    window = max(containing) if containing else max(groups)
    return groups[window]


def compute_feeder_routes(stops, stop_routes, routes):
    """Map each subway station to the surface routes that feed it.

    Subway stations are identified as the parent stations of platform stops
    served by route_type=1 trips — location_type=1 alone is NOT reliable in
    the TTC merged feed (it also marks bus terminal loops like Westmore).

    Returns {(station_id, route_short_name): (station_name, min_distance_m)}.
    """
    subway_route_ids = {
        rid for rid, (_, rtype) in routes.items() if rtype == SUBWAY_ROUTE_TYPE
    }

    station_ids = set()
    for stop_id, rids in stop_routes.items():
        if rids & subway_route_ids and stop_id in stops:
            parent = stops[stop_id][4]
            if parent and parent in stops:
                station_ids.add(parent)

    # Surface stops: anything that has service from at least one non-subway
    # route and is not itself a station node.
    surface_stops = []
    for stop_id, rids in stop_routes.items():
        info = stops.get(stop_id)
        if info is None or info[3] == "1":
            continue
        surface_rids = rids - subway_route_ids
        if surface_rids:
            surface_stops.append((stop_id, info[0], info[1], surface_rids))

    feeders = {}
    # ~0.003 degrees latitude ≈ 330 m; cheap bounding-box prefilter so we
    # don't run haversine against every stop in the city for every station.
    lat_margin = math.degrees(FEEDER_RADIUS_M / EARTH_RADIUS_M) * 1.2
    for station_id in station_ids:
        s_lat, s_lon, s_name = stops[station_id][0], stops[station_id][1], stops[station_id][2]
        for _, lat, lon, surface_rids in surface_stops:
            if abs(lat - s_lat) > lat_margin:
                continue
            distance = haversine_m(s_lat, s_lon, lat, lon)
            if distance > FEEDER_RADIUS_M:
                continue
            for rid in surface_rids:
                short_name = routes[rid][0] or rid
                key = (station_id, short_name)
                existing = feeders.get(key)
                if existing is None or distance < existing[1]:
                    feeders[key] = (s_name, distance)

    return station_ids, feeders


def main():
    zf = download_gtfs(GTFS_URL)

    print("parsing routes.txt ...", flush=True)
    routes = {}
    for row in read_csv(zf, "routes.txt"):
        routes[row["route_id"]] = (
            row.get("route_short_name", ""),
            int(row["route_type"]) if row.get("route_type") else None,
        )
    print(f"  {len(routes)} routes", flush=True)

    weekday_services = select_weekday_services(zf)
    if weekday_services is None:
        print("calendar.txt missing or empty; counting all trips", flush=True)
    else:
        print(f"restricting to {len(weekday_services)} weekday service pattern(s)", flush=True)

    print("parsing trips.txt ...", flush=True)
    # trip_to_route drives headway computation (weekday service only); all_trips
    # feeds the trips lookup table used by the crowding estimator to resolve
    # direction_id from a live trip_id, because the TTC GTFS-RT feed does not
    # populate trip.direction_id. Live trips can belong to any service day, so
    # this table is NOT filtered to weekday services.
    trip_to_route = {}
    all_trips = []
    for row in read_csv(zf, "trips.txt"):
        route_id = row["route_id"]
        direction = row.get("direction_id", "")
        all_trips.append((
            row["trip_id"],
            route_id,
            int(direction) if direction.strip() else None,
        ))
        if weekday_services is not None and row.get("service_id") not in weekday_services:
            continue
        trip_to_route[row["trip_id"]] = (route_id, direction or "0")
    print(f"  {len(all_trips)} trips total, {len(trip_to_route)} weekday trips", flush=True)

    print("parsing stops.txt ...", flush=True)
    # stop_id -> (lat, lon, name, location_type, parent_station)
    stops = {}
    for row in read_csv(zf, "stops.txt"):
        try:
            lat, lon = float(row["stop_lat"]), float(row["stop_lon"])
        except (KeyError, ValueError):
            continue
        stops[row["stop_id"]] = (
            lat, lon, row.get("stop_name", ""),
            row.get("location_type", "").strip(),
            row.get("parent_station", "").strip(),
        )
    print(f"  {len(stops)} stops", flush=True)

    all_trip_to_route = {trip_id: route_id for trip_id, route_id, _ in all_trips}

    print("parsing stop_times.txt (streaming, this is the big one) ...", flush=True)
    # One pass accumulates two things: the first stop of each trip (lowest
    # stop_sequence + its arrival_time) for headway computation, and the set
    # of routes serving each stop (across all service days) for the
    # feeder-route mapping.
    trip_first_arrival = {}
    stop_routes = defaultdict(set)
    rows = 0
    for row in read_csv(zf, "stop_times.txt"):
        rows += 1
        trip_id = row["trip_id"]

        route_id = all_trip_to_route.get(trip_id)
        if route_id is not None:
            stop_routes[row["stop_id"]].add(route_id)

        arrival = row.get("arrival_time", "").strip()
        if not arrival:
            continue
        seq = int(row["stop_sequence"])
        current = trip_first_arrival.get(trip_id)
        if current is None or seq < current[0]:
            trip_first_arrival[trip_id] = (seq, arrival)
    print(f"  {rows} stop_time rows, {len(trip_first_arrival)} trips with arrivals, "
          f"{len(stop_routes)} stops with service", flush=True)

    # Count trips per route per direction per hour, based on each trip's first
    # departure. Keyed by route_short_name (e.g. "504"), not the TTC-internal
    # GTFS route_id, because the GTFS-RT vehicle-positions feed identifies
    # routes by number — this is the key the delay predictor joins on.
    trips_per_hour = defaultdict(int)
    for trip_id, (_, arrival) in trip_first_arrival.items():
        mapping = trip_to_route.get(trip_id)
        if mapping is None:
            continue
        route_id, direction = mapping
        if route_id not in routes:
            continue
        short_name = routes[route_id][0] or route_id
        trips_per_hour[(short_name, parse_hour(arrival), direction)] += 1

    # Headway is a per-direction quantity: summing both directions would halve
    # it artificially. Use the busier direction's trip count per (route, hour).
    max_dir_count = defaultdict(int)
    for (route, hour, _), count in trips_per_hour.items():
        key = (route, hour)
        max_dir_count[key] = max(max_dir_count[key], count)

    headways = {
        key: 60.0 / count for key, count in max_dir_count.items() if count > 0
    }
    print(f"computed {len(headways)} (route, hour) headway entries", flush=True)

    station_ids, feeders = compute_feeder_routes(stops, stop_routes, routes)
    print(f"computed {len(feeders)} feeder-route relationships "
          f"for {len(station_ids)} subway stations (radius {FEEDER_RADIUS_M}m)", flush=True)

    conn = db_connect()
    conn.autocommit = False
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS routes (
              route_id TEXT PRIMARY KEY,
              route_short_name TEXT,
              route_type INTEGER
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_headways (
              route_id TEXT NOT NULL,
              hour_of_day INTEGER NOT NULL,
              scheduled_headway_minutes DOUBLE PRECISION NOT NULL,
              PRIMARY KEY (route_id, hour_of_day)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
              trip_id TEXT PRIMARY KEY,
              route_id TEXT NOT NULL,
              direction_id INTEGER
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS station_feeder_routes (
              station_id TEXT NOT NULL,
              station_name TEXT NOT NULL,
              route_id TEXT NOT NULL,
              distance_meters DOUBLE PRECISION NOT NULL,
              PRIMARY KEY (station_id, route_id)
            );
            """
        )

        # Full refresh: stale rows from a previous feed version would
        # otherwise linger forever, since we only upsert below.
        cur.execute("DELETE FROM scheduled_headways;")
        cur.execute("DELETE FROM trips;")
        cur.execute("DELETE FROM station_feeder_routes;")

        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO station_feeder_routes (station_id, station_name, route_id, distance_meters) VALUES %s",
            [
                (station_id, name, short_name, round(distance, 1))
                for (station_id, short_name), (name, distance) in feeders.items()
            ],
            page_size=1000,
        )

        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO trips (trip_id, route_id, direction_id) VALUES %s ON CONFLICT (trip_id) DO NOTHING",
            all_trips,
            page_size=5000,
        )

        for route_id, (short_name, route_type) in routes.items():
            cur.execute(
                """
                INSERT INTO routes (route_id, route_short_name, route_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (route_id) DO UPDATE
                  SET route_short_name = EXCLUDED.route_short_name,
                      route_type = EXCLUDED.route_type;
                """,
                (route_id, short_name, route_type),
            )

        for (route_id, hour), headway in headways.items():
            cur.execute(
                """
                INSERT INTO scheduled_headways (route_id, hour_of_day, scheduled_headway_minutes)
                VALUES (%s, %s, %s)
                ON CONFLICT (route_id, hour_of_day) DO UPDATE
                  SET scheduled_headway_minutes = EXCLUDED.scheduled_headway_minutes;
                """,
                (route_id, hour, round(headway, 2)),
            )
    conn.close()

    print(f"\nloaded {len(routes)} routes, {len(all_trips)} trips, "
          f"{len(headways)} scheduled headway rows, and {len(feeders)} "
          f"feeder-route rows ({len(station_ids)} subway stations) into TimescaleDB")

    # Sanity check: feeder routes for Bloor-Yonge
    print("\nBloor-Yonge feeder routes (surface routes within "
          f"{FEEDER_RADIUS_M}m):")
    by_feeders = sorted(
        (short, name, dist)
        for (sid, short), (name, dist) in feeders.items()
        if "bloor-yonge" in name.lower()
    )
    if by_feeders:
        for short, _, dist in by_feeders:
            print(f"  route {short:>4}  nearest stop {dist:.0f}m")
    else:
        print("  (none found — check station identification)")

    # Summary for route 504 (King streetcar)
    print("\nroute 504 (King) scheduled headways by hour:")
    print(f"  {'hour':>4}  {'headway (min)':>13}")
    found = False
    for hour in range(24):
        headway = headways.get(("504", hour))
        if headway is not None:
            print(f"  {hour:>4}  {headway:>13.2f}")
            found = True
    if not found:
        print("  (no headway entries found for route 504)")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - one-shot job, fail loudly with a clear message
        print(f"gtfs-loader failed: {exc}", file=sys.stderr)
        sys.exit(1)
