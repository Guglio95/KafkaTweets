package it.polimi.middleware;

import it.polimi.middleware.model.TimestampedEvent;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static junit.framework.TestCase.assertEquals;

public class SlidingWindowTest {

    @org.junit.Test
    public void store() {
        //Test 0
        SlidingWindow slidingWindow = new SlidingWindow(5);

        slidingWindow.store(new TestSlidingWindows(1, "18:00:01"));
        slidingWindow.store(new TestSlidingWindows(2, "18:01:02"));
        slidingWindow.store(new TestSlidingWindows(3, "18:02:03"));
        slidingWindow.store(new TestSlidingWindows(4, "18:03:04"));
        slidingWindow.store(new TestSlidingWindows(5, "18:01:05"));//Out of seq
        slidingWindow.store(new TestSlidingWindows(6, "18:04:06"));
        slidingWindow.store(new TestSlidingWindows(7, "18:05:07"));
        slidingWindow.store(new TestSlidingWindows(8, "18:06:10"));
        slidingWindow.store(new TestSlidingWindows(9, "18:04:09"));//Out of seq.
        slidingWindow.store(new TestSlidingWindows(10, "18:08:50"));
        slidingWindow.store(new TestSlidingWindows(11, "18:10:10"));//Out of seq

        assertEquals(((TestSlidingWindows) (slidingWindow.getWindow().get(0))).id, 11);
        assertEquals(((TestSlidingWindows) (slidingWindow.getWindow().get(1))).id, 10);
        assertEquals(((TestSlidingWindows) (slidingWindow.getWindow().get(2))).id, 8);
        assertEquals(((TestSlidingWindows) (slidingWindow.getWindow().get(3))).id, 7);
        assertEquals((slidingWindow.getWindow().size()), 4);

        // Test 2
        slidingWindow = new SlidingWindow(5);

        slidingWindow.store(new TestSlidingWindows(21, "18:00:01"));
        slidingWindow.store(new TestSlidingWindows(22, "18:01:00"));
        slidingWindow.store(new TestSlidingWindows(23, "18:10:10"));
        slidingWindow.store(new TestSlidingWindows(24, "18:09:00"));
        slidingWindow.store(new TestSlidingWindows(25, "18:01:00"));
        slidingWindow.store(new TestSlidingWindows(26, "18:39:03"));

        assertEquals(((TestSlidingWindows) (slidingWindow.getWindow().get(0))).id, 26);
        assertEquals((slidingWindow.getWindow().size()), 1);
    }

    private class TestSlidingWindows implements TimestampedEvent {
        private int id;
        private long timestamp;

        public TestSlidingWindows(int id, String timestamp) {
            String pattern = "yyyy-MM-dd HH:mm:ss";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            Date date = null;
            try {
                date = simpleDateFormat.parse("2019-01-24 " + timestamp);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            this.id = id;
            this.timestamp = date.toInstant().toEpochMilli() / 1000L;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            Date date = new java.util.Date(timestamp * 1000L);
            SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

            return "TestSlidingWindows{" +
                    "id=" + id +
                    ", timestamp=" + sdf.format(date) +
                    '}';
        }
    }
}