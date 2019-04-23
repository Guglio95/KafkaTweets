package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetFilter;
import it.polimi.middleware.model.TweetValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

class TweetProducer {
    private static final Logger logger = Logger.getLogger(TweetProducer.class);

    private KafkaProducer<String, String> producer;
    private Gson gson = new Gson();
    private final String kafka1_URL;

    TweetProducer(String kafka1_URL) {
        this.kafka1_URL = kafka1_URL;
        this.producer = createProducer();
    }

    private void produce(TweetFilter tweetFilter, String key, TweetValue value) {
        logger.info("Publishing to topic " + getTopic(tweetFilter) + " the content " + value.toString());
        ProducerRecord<String, String> record = new ProducerRecord<>(getTopic(tweetFilter), key, gson.toJson(value));
        this.producer.send(record);
    }

    private void produce(TweetFilter tweetFilter, String key, TweetValue value, int partition) {
        logger.info("Publishing to topic " + getTopic(tweetFilter) + " partition " + partition + " the content " + value.toString());
        ProducerRecord<String, String> record = new ProducerRecord<>(getTopic(tweetFilter), partition, key, gson.toJson(value));
        this.producer.send(record);
    }


    void enqueue(TweetValue tweetValue) {
        //Publish to partition for each mention.
        tweetValue.getMentions().stream()
                .filter(m -> m.length() > 0)
                .map(key -> CustomPartitioner.partition(partitionsPerTopic(TweetFilter.MENTION), key))
                .distinct()
                .forEach(partition -> produce(TweetFilter.MENTION, "key", tweetValue, partition));

        //Publish to partition for each tag
        tweetValue.getTags().stream()
                .filter(m -> m.length() > 0)
                .map(key -> CustomPartitioner.partition(partitionsPerTopic(TweetFilter.TAG), key))
                .distinct()
                .forEach(partition -> produce(TweetFilter.TAG, "key", tweetValue, partition));

        //Publish to location queue
        produce(TweetFilter.LOCATION, tweetValue.getLocation(), tweetValue);
    }

    int partitionsPerTopic(TweetFilter tag) {
        return producer.partitionsFor(getTopic(tag)).size();
    }

    void stop() {
        producer.flush();
        producer.close(1, TimeUnit.SECONDS);
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka1_URL);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());


        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(props);
    }

    private String getTopic(TweetFilter tweetFilter) {
        return tweetFilter.toString().toLowerCase();
    }
}
