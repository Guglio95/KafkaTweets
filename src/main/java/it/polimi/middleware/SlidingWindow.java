package it.polimi.middleware;

import it.polimi.middleware.model.TimestampedEvent;

import java.time.Duration;
import java.util.*;

public class SlidingWindow {
    private Set<TimestampedEvent> events;
    private long size;
    private long hop;
    private long openTime;
    private long closeTime;
    private SlidingWindowTrigger trigger;

    SlidingWindow(Duration size, Duration hop) {
        this.events = new TreeSet<>(Comparator.comparingLong(TimestampedEvent::getTimestamp));
        this.size = size.toMillis();
        this.hop = hop.toMillis();
        this.closeTime = System.currentTimeMillis();
        this.openTime = this.closeTime - this.size;
        this.trigger = new SlidingWindowTrigger(this.hop, this);
        this.trigger.start();
    }

    synchronized void store(TimestampedEvent event) {
        if (event.getTimestamp() > this.openTime) {
            events.add(event);
            if (event.getTimestamp() > this.closeTime) {
                this.closeTime = event.getTimestamp();
                this.openTime = this.closeTime - openTime;
                removeOldEvents();
            }
        }
    }

    synchronized void slide() {
        this.closeTime = System.currentTimeMillis();
        this.openTime = this.closeTime - size;
        removeOldEvents();
    }

    synchronized List<TimestampedEvent> getWindow() {
        return new ArrayList<>(events);
    }

    void stop() {
        try {
            this.trigger.terminate();
            this.trigger.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void removeOldEvents() {
        //Remove events older then X minutes.
        events.removeIf(timestampedEvent -> timestampedEvent.getTimestamp() < this.openTime);
    }

    private static class SlidingWindowTrigger extends Thread {
        private long hop;
        private SlidingWindow window;
        private volatile boolean running;

        SlidingWindowTrigger(long hop, SlidingWindow window) {
            this.hop = hop;
            this.window = window;
            this.running = true;
        }

        @Override
        public void run() {
            try {
                while (running) {
                    Thread.sleep(hop);
                    window.slide();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void terminate() {
            this.running = false;
        }
    }
}