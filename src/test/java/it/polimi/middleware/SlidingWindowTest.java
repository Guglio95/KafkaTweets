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

        slidingWindow.store(new TestElement(1, "18:00:01"));
        slidingWindow.store(new TestElement(2, "18:01:02"));
        slidingWindow.store(new TestElement(3, "18:02:03"));
        slidingWindow.store(new TestElement(4, "18:03:04"));
        slidingWindow.store(new TestElement(5, "18:01:05"));//Out of seq
        slidingWindow.store(new TestElement(6, "18:04:06"));
        slidingWindow.store(new TestElement(7, "18:05:07"));
        slidingWindow.store(new TestElement(8, "18:06:10"));
        slidingWindow.store(new TestElement(9, "18:04:09"));//Out of seq.
        slidingWindow.store(new TestElement(10, "18:08:50"));
        slidingWindow.store(new TestElement(11, "18:10:10"));

        //Returned elements are 11,10,8,7
        assertEquals(((TestElement) (slidingWindow.getWindow().get(0))).id, 11);
        assertEquals(((TestElement) (slidingWindow.getWindow().get(1))).id, 10);
        assertEquals(((TestElement) (slidingWindow.getWindow().get(2))).id, 8);
        assertEquals(((TestElement) (slidingWindow.getWindow().get(3))).id, 7);
        assertEquals((slidingWindow.getWindow().size()), 4);

        // Test 2
        slidingWindow = new SlidingWindow(5);

        slidingWindow.store(new TestElement(21, "18:00:01"));
        slidingWindow.store(new TestElement(22, "18:01:00"));
        slidingWindow.store(new TestElement(23, "18:10:10"));
        slidingWindow.store(new TestElement(24, "18:09:00"));
        slidingWindow.store(new TestElement(25, "18:01:00"));
        slidingWindow.store(new TestElement(26, "18:39:03"));

        assertEquals(((TestElement) (slidingWindow.getWindow().get(0))).id, 26);
        assertEquals((slidingWindow.getWindow().size()), 1);


        //Test 3
        slidingWindow = new SlidingWindow(5);

        slidingWindow.store(new TestElement(21, "18:00:01"));
        slidingWindow.store(new TestElement(22, "18:01:00"));
        slidingWindow.store(new TestElement(23, "18:10:10"));
        slidingWindow.store(new TestElement(24, "18:09:00"));
        slidingWindow.store(new TestElement(25, "18:01:00"));

        assertEquals(((TestElement) (slidingWindow.getWindow().get(0))).id, 23);
        assertEquals((slidingWindow.getWindow().size()), 1);


        //Test 4
        slidingWindow = new SlidingWindow(5);

        slidingWindow.store(new TestElement(21, "18:00:10"));
        slidingWindow.store(new TestElement(22, "18:00:00"));
        slidingWindow.store(new TestElement(23, "18:01:10"));
        slidingWindow.store(new TestElement(24, "18:05:05"));
        slidingWindow.store(new TestElement(25, "18:03:00"));
        slidingWindow.store(new TestElement(25, "18:05:00"));

        assertEquals(((TestElement) (slidingWindow.getWindow().get(0))).id, 25);
        assertEquals(((TestElement) (slidingWindow.getWindow().get(1))).id, 24);
        assertEquals(((TestElement) (slidingWindow.getWindow().get(2))).id, 23);
        assertEquals(((TestElement) (slidingWindow.getWindow().get(3))).id, 22);
        assertEquals(((TestElement) (slidingWindow.getWindow().get(4))).id, 21);
        assertEquals((slidingWindow.getWindow().size()), 5);


    }

    /**
     * Test element to push on sliding window
     */
    private class TestElement implements TimestampedEvent {
        private int id;
        private long timestamp;

        public TestElement(int id, String timestamp) {
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

            return "TestElement{" +
                    "id=" + id +
                    ", timestamp=" + sdf.format(date) +
                    '}';
        }
    }
}