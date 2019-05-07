package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetValue;
import it.polimi.middleware.persistance.TopicPersistance;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class TweetConsumer {
    private static final Logger logger = Logger.getLogger(TweetConsumer.class);

    private final String kafkaURL;
    private String topic;

    private TopicPersistance topicPersistance;
    private Map<Integer, SlidingWindow> slidingWindows;
    private KafkaConsumer<String, String> consumer;

    private volatile boolean running;
    private boolean hasSpinned;//True if consumer has spinned at least once.
    private Gson gson = new Gson();
    private Map<Integer, Set<TweetObserver>> partitionsObservers = new ConcurrentHashMap<>();

    TweetConsumer(String kafkaURL, String topic) {
        this.kafkaURL = kafkaURL;
        this.topicPersistance = new TopicPersistance(topic);
        this.slidingWindows = new HashMap<>();
        this.topic = topic;
        this.consumer = createKafkaConsumer();
        this.running = false;
    }

    void blockingStart() {
        synchronized (this) {
            logger.info("Starting consumer.");
            start();
            logger.info("Waiting for consumer to fetch last tweets.");

            //Wait for the consumer to spin at least once; this means that if there are tweets we have downloaded them.
            while (!hasSpinned) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for Consumer start", e);
                }
            }
            logger.info("Consumer is ready.");
        }
    }


    void start() {
        coldStart();
        this.running = true;
        new Thread(this::consume).start();
    }

    private void coldStart() {
        this.consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(1);

        consumer.assignment().forEach(topicPartition -> {

            //Set the offset for each partition.
            Long lastCommittedOffset = topicPersistance.getOffset(topicPartition.partition());
            if (lastCommittedOffset == null) {
                lastCommittedOffset = 0L; // Offset was never committed.
            } else {
                lastCommittedOffset += 1L; // Offset was committed last time; start reading from offset+1

            }
            consumer.seek(topicPartition, lastCommittedOffset);
            logger.info("Setting offset to " + lastCommittedOffset + " per partition " + topicPartition.partition());

            //Create a new sliding windows for this partition.
            final SlidingWindow slidingWindow = new SlidingWindow(Duration.ofMinutes(5), Duration.ofSeconds(10));
            slidingWindows.put(topicPartition.partition(), slidingWindow);

            //Load all the previously red tweets in the sliding window.
            topicPersistance.readAllTweets(topicPartition.partition())
                    .forEach(tweetValue -> {
                        slidingWindow.store(tweetValue);
                        logger.info("Cold start: loading " + tweetValue.toString() + " to sliding window per " +
                                "partition " + topicPartition.partition());
                    });
        });
    }

    private void consume() {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    logger.info("Partition '" + record.partition() + "' found at offset '" + record.offset() +
                            "' content: '" + record.value() + "'");

                    //Deserialize the received tweet.
                    TweetValue tweet = gson.fromJson(record.value(), TweetValue.class);

                    //Store tweets to our reliable database and sliding window
                    topicPersistance.storeTweet(tweet, record.partition(), record.offset());
                    slidingWindows.get(record.partition()).store(tweet);

                    //Notify our partitionsObservers (if any)
                    if (partitionsObservers.containsKey(record.partition()))
                        partitionsObservers.get(record.partition()).forEach(TweetObserver::newTweetReceived);
                }
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();//Offset of the last received record

                //Since we have stored the tweet we can commit the increased offset.
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }

            if (!hasSpinned) {//Notify that this consumer is ready since has spinned for the first time.
                hasSpinned = true;
                logger.info("Consumer for topic " + topic + " has just spinned for the first time");
                synchronized (this) {
                    notifyAll();
                }
            }
        }
        tearDownKafkaConsumer();
    }

    void stop() {
        this.running = false;
    }

    private void tearDownKafkaConsumer() {
        this.consumer.close();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + topic);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaConsumer<>(props);
    }

    TopicPersistance getTopicPersistance() {
        return topicPersistance;
    }

    Map<Integer, SlidingWindow> getSlidingWindows() {
        return slidingWindows;
    }

    void addObserver(TweetObserver tweetObserver, int partitionId) {
        partitionsObservers.putIfAbsent(partitionId, ConcurrentHashMap.newKeySet());
        partitionsObservers.get(partitionId).add(tweetObserver);

        logger.info("A new observer has registered to topic " + topic + ", now in partition " + partitionId + " we have "
                + partitionsObservers.get(partitionId).size() + " partitionsObservers.");
    }

    void removeObserver(TweetObserver tweetObserver) {
        partitionsObservers.forEach((partitionID, observerSet) -> {
            if (observerSet.remove(tweetObserver))
                logger.info("An observer has gone from topic " + topic + " now in partition " + partitionID
                        + " we have " + observerSet.size() + " partitionsObservers");
        });
    }
}
