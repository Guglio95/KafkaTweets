package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetValue;
import it.polimi.middleware.persistance.TweetPersistance;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

class TweetConsumer {
    private static final Logger logger = Logger.getLogger(TweetConsumer.class);

    private final String kafka1_URL;
    private String topic;
    private TweetPersistance tweetPersistance;
    private SlidingWindow slidingWindow;
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running;
    private Gson gson = new Gson();
    private Queue<TweetObserver> observers = new ConcurrentLinkedQueue<>();

    TweetConsumer(String kafka1_URL, String topic) {
        this.kafka1_URL = kafka1_URL;
        this.tweetPersistance = new TweetPersistance(topic);
        this.slidingWindow = new SlidingWindow(5);
        this.topic = topic;
        this.consumer = createKafkaConsumer();
        this.running = false;
    }

    public void start() {
        coldStart();
        this.running = true;
        new Thread(this::consume).start();
        synchronized (this) {
            notifyAll();
        }
    }

    public void blockingStart() {
        synchronized (this) {
            logger.info("Starting consumer.");
            start();
            logger.info("Waiting for consumer to fetch last tweets.");
            try {
                wait(10000);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for Consumer start", e);
            }
            logger.info("Consumer is ready.");
        }
    }

    private void coldStart() {
        //Read all tweets from local database and load it in sliding window.

        tweetPersistance.readAll().forEach(tweetValue -> {
            slidingWindow.store(tweetValue);
            logger.info("Cold start: loading " + tweetValue.toString() + " to sliding window.");
        });

        //Set consumer offset to stored offset (if any)
        long storedOffset = tweetPersistance.getOffset();
        if (storedOffset > 0) {
            //If we have an offset record, we can start reading again from n-th +1 record.
            setUpConsumer(storedOffset + 1);
        } else {
            //If offset is not set we can start seeking from the beginning.
            setUpConsumer(0);
        }
    }

    private void consume() {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    logger.info("Found at offset: '" + record.offset() + "' content: '" + record.value() + "'");
                    TweetValue tweet = gson.fromJson(record.value(), TweetValue.class);

                    //Store tweets to our reliable database and sliding window
                    tweetPersistance.write(tweet, record.offset());
                    slidingWindow.store(tweet);

                    //Notify our observers (if any)
                    observers.forEach(observer -> observer.receive(tweet));
                }
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

                //Since we have stored the tweet we can commit the increased offset.
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }
        tearDownKafkaConsumer();
    }


    void stop() {
        this.running = false;
    }

    private void setUpConsumer(long readOffset) {
        this.consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(1);

        consumer.assignment().forEach(topicPartition -> {
            consumer.seek(topicPartition, readOffset);
            logger.info("Setting offset to " + readOffset + " per partition " + topicPartition.topic());
        });
    }

    private void tearDownKafkaConsumer() {
        this.consumer.close();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka1_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consgroup1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
//        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaConsumer<>(props);
    }

    public TweetPersistance getTweetPersistance() {
        return tweetPersistance;
    }

    public SlidingWindow getSlidingWindow() {
        return slidingWindow;
    }

    public void addObserver(TweetObserver tweetObserver) {
        observers.add(tweetObserver);
        logger.info("A new observer has registered to topic "+topic+", now we have "+observers.size()+" observers.");
    }

    public void removeObserver(TweetObserver tweetObserver) {
        observers.remove(tweetObserver);
        logger.info("An observer has gone from topic "+topic+", now we have "+observers.size()+" observers.");
    }
}
