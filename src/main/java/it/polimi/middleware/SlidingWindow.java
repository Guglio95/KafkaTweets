package it.polimi.middleware;

import it.polimi.middleware.model.TimestampedEvent;

import java.util.ArrayList;
import java.util.List;

public class SlidingWindow {
    private List<TimestampedEvent> eventsList = new ArrayList<>();
    int windowSizeInMinutes;

    SlidingWindow(int windowSizeInMinutes) {
        this.windowSizeInMinutes = windowSizeInMinutes;
    }

    synchronized void store(TimestampedEvent event) {
        //Add event to windows if this happened in last X minutes.
        if (event.getTimestamp() > timestampMinutesAgo(windowSizeInMinutes)) {
            eventsList.add(event);
        }
    }

    synchronized List<TimestampedEvent> getWindow() {
        removeOldEntries();
        eventsList.sort((t0, t1) -> (int) (t1.getTimestamp() - t0.getTimestamp()));
        return new ArrayList<>(eventsList);
    }

    private void removeOldEntries() {
        //Remove events older then X minutes.
        eventsList.removeIf(timestampedEvent -> timestampedEvent.getTimestamp() < timestampMinutesAgo(windowSizeInMinutes));
    }

    private long timestampMinutesAgo(int minutes) {
        return System.currentTimeMillis() / 1000L - minutes * 60;
    }
}