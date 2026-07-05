package com.ttcintelligence.rippledetector;

/**
 * One row of station_feeder_routes: a surface route that feeds a subway
 * station, with the distance from the station to the route's nearest stop.
 */
public record FeederRoute(String stationId, String stationName, String routeId, double distanceMeters) {
}
