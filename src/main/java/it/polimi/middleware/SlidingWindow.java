package it.polimi.middleware;

import it.polimi.middleware.model.TimestampedEvent;

import java.util.ArrayList;
import java.util.List;

public class SlidingWindow {
    private List<TimestampedEvent> window[];
    private long lastTimestamp = Long.MIN_VALUE;
    int lastKeyWritten = 0;

    SlidingWindow(int minutes) {
        this.window = new List[minutes + 1];
    }

    synchronized void store(TimestampedEvent event) {
        int key = (int) ((event.getTimestamp() / 60) % this.window.length);

        //If we are in the same minute, append to the existing list.
        if ((int) (event.getTimestamp() / 60) == (int) (lastTimestamp / 60)) {

            //If current slot is empty, create a new list.
            if (this.window[key] == null)
                this.window[key] = new ArrayList<>();

            this.window[key].add(0, event);//Append last received event.
            this.lastKeyWritten = key;
            lastTimestamp = event.getTimestamp();
        }
        //If since last received message it has passed more than the size of the sliding windows we can trash everything.
        else if (event.getTimestamp() > lastTimestamp + 60 * window.length) {
            for (int i = 0; i < window.length; i++)
                window[i] = new ArrayList<>();

            this.window[key].add(0, event);
            this.lastKeyWritten = key;
            lastTimestamp = event.getTimestamp();
        }
        //If the minute is not the same because it's in the future
        else if (event.getTimestamp() >= lastTimestamp) {

            //Delete previous stored tweets from "lastWrittenKey" to the key that will now be written.
            //Example:  if last time we wrote to key "2" and now we should write to key "4" this for will empty
            //          the array position 3
            //Example2: if last time we wrote to key "4" and now we should write to key "3" (assuming window = 5)
            //          this for will empty array position 1,2

            for (int i = (lastKeyWritten + 1) % window.length; i != key; i = (i + 1) % window.length) {
                this.window[i] = new ArrayList<>();
            }

            this.window[key] = new ArrayList<>();
            this.window[key].add(0, event);//Add just received event.

            this.lastKeyWritten = key;
            this.lastTimestamp = event.getTimestamp();
        } else if (event.getTimestamp() < lastTimestamp) {
            //Out of sequence, ignore element.
        }
    }

    List<TimestampedEvent> getWindow() {
        //Cycle each slot of the array starting from the last written:
        //Example: if last written slot is "2" and array has lenght=5 the visiting will be: 2,1,0,4,3
        List<TimestampedEvent> out = new ArrayList<>();
        for (int i = 0; i < window.length; i++) {
            int visitingKey = Math.floorMod((lastKeyWritten - i), window.length);

            if (window[visitingKey] != null)
                window[visitingKey].forEach(out::add);
        }
        return out;
    }
}