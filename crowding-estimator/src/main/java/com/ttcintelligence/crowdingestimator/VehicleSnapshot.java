package com.ttcintelligence.crowdingestimator;

/**
 * Latest known position of one vehicle within a window: its stop sequence and
 * the feed timestamp it was observed at. Jackson-serialized by
 * {@link VehicleMapSerde} for the window store and its changelog topic.
 */
public class VehicleSnapshot {

    public int stopSequence;
    public long timestamp;

    public VehicleSnapshot() {
    }

    public VehicleSnapshot(int stopSequence, long timestamp) {
        this.stopSequence = stopSequence;
        this.timestamp = timestamp;
    }
}
