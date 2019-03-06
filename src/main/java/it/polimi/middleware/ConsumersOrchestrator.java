package it.polimi.middleware;

import org.apache.log4j.Logger;

import java.util.HashMap;

public class ConsumersOrchestrator {
    private final HashMap<String, TweetConsumer> consumers = new HashMap<>();

    private static final Logger logger = Logger.getLogger(ConsumersOrchestrator.class);

    /**
     * Retrieves the consumer associated to the given topic.
     *
     * @param topic
     * @return
     */
    public synchronized TweetConsumer getConsumerPerTopic(String topic) {
        if (!consumers.containsKey(topic)) {
            logger.info("Consumer per " + topic + " is not existing, so I'll create it.");
            TweetConsumer consumer = new TweetConsumer(topic);
            consumers.put(topic, consumer);
            consumer.blockingStart();//Wait untill consumer is ready
        }
        return consumers.get(topic);
    }
}
