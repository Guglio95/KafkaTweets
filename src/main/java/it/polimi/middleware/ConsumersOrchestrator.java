package it.polimi.middleware;

import it.polimi.middleware.model.TweetFilter;
import org.apache.log4j.Logger;

import java.util.HashMap;

public class ConsumersOrchestrator {
    private static final Logger logger = Logger.getLogger(ConsumersOrchestrator.class);

    private final HashMap<String, TweetConsumer> consumers = new HashMap<>();
    private final String kafka1_URL;

    public ConsumersOrchestrator(String kafka1_URL) {
        this.kafka1_URL = kafka1_URL;
    }

    /**
     * Retrieves the consumer associated to given topic.
     *
     * @param topic
     * @return
     */
    private synchronized TweetConsumer getConsumerPerTopic(String topic) {
        if (!consumers.containsKey(topic)) {
            logger.info("Consumer per " + topic + " is not existing, so I'll create it.");
            TweetConsumer consumer = new TweetConsumer(kafka1_URL, topic);
            consumers.put(topic, consumer);
            consumer.blockingStart();//Wait untill consumer is ready
            logger.info("Consumer is ready to be used.");
        }
        return consumers.get(topic);
    }

    /**
     * Retrieves the consumer associated to given filter and query.
     *
     * @param filter
     * @return
     */
    public TweetConsumer getConsumer(TweetFilter filter) {
        return getConsumerPerTopic(filter.toString());
    }
}
