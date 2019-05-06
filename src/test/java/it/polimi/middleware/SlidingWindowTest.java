package it.polimi.middleware;

import it.polimi.middleware.model.TimestampedEvent;

import java.text.SimpleDateFormat;
import java.util.Date;

import static junit.framework.TestCase.assertEquals;

public class SlidingWindowTest {

    @org.junit.Test
    public void store() {
        //Test 0
        SlidingWindow slidingWindow = new SlidingWindow(5);

        slidingWindow.store(new TestElement(1, 0));
        slidingWindow.store(new TestElement(2, 1.5));
        slidingWindow.store(new TestElement(3, 2.3));
        slidingWindow.store(new TestElement(4, 1.2));
        slidingWindow.store(new TestElement(5, 4.3));
        slidingWindow.store(new TestElement(6, 2.6));
        slidingWindow.store(new TestElement(7, 3.5));
        slidingWindow.store(new TestElement(8, 6));
        slidingWindow.store(new TestElement(9, 5.4));
        slidingWindow.store(new TestElement(10, 0.5));

        assertEquals(1, ((TestElement) (slidingWindow.getWindow().get(0))).getId());
        assertEquals(10, ((TestElement) (slidingWindow.getWindow().get(1))).getId());
        assertEquals(4, ((TestElement) (slidingWindow.getWindow().get(2))).getId());
        assertEquals(2, ((TestElement) (slidingWindow.getWindow().get(3))).getId());
        assertEquals(3, ((TestElement) (slidingWindow.getWindow().get(4))).getId());
        assertEquals(6, ((TestElement) (slidingWindow.getWindow().get(5))).getId());
        assertEquals(7, ((TestElement) (slidingWindow.getWindow().get(6))).getId());
        assertEquals(5, ((TestElement) (slidingWindow.getWindow().get(7))).getId());
    }

    /**
     * Test element to push on sliding window
     */
    private class TestElement extends TimestampedEvent {
        private int id;

        TestElement(int id, double minutesAgo) {
            super((int) (System.currentTimeMillis() / 1000L - 60 * minutesAgo));
            this.id = id;
        }

        int getId() {
            return id;
        }

        @Override
        public String toString() {
            Date date = new java.util.Date(getTimestamp() * 1000L);
            SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

            return "TestElement{" +
                    "id=" + id +
                    ", timestamp=" + sdf.format(date) +
                    '}';
        }
    }
}