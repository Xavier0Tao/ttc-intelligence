package com.ttcintelligence.crowdingestimator;

public class Main {

    // Consumes the `vehicle-positions` topic, infers relative crowding levels
    // from the headway gap between consecutive vehicles on the same route
    // (a widening gap implies passenger buildup at upcoming stops), and
    // publishes crowding estimates to the `crowding-estimates` topic.
    public static void main(String[] args) {
    }
}
