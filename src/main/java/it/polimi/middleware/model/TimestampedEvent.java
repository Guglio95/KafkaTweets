package it.polimi.middleware.model;

public abstract class TimestampedEvent {
    private long timestamp;

    public TimestampedEvent(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
