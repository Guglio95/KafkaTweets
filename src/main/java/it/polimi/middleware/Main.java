package it.polimi.middleware;

import it.polimi.middleware.consumer.Consumer;
import it.polimi.middleware.producer.Producer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {

    public static void main(String[] args) {
        String topic = "tweets";

        Logger.getRootLogger().setLevel(Level.INFO);//Replace to .INFO for debug messages.

        Producer producer = new Producer("localhost:9092", "transaction-id", topic);
        producer.enqueue(System.currentTimeMillis() + "", "CIAOX");
        producer.close();

        Consumer consumer = new Consumer(topic, "testgroup");
    }
}
