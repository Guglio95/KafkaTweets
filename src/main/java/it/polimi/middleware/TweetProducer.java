package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetFilter;
import it.polimi.middleware.model.TweetValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

class TweetProducer {
    private static final Logger logger = Logger.getLogger(TweetProducer.class);
    public static final String TRANSACTIONAL_ID = "tweet-producer";

    private KafkaProducer<String, String> producer;
    private Gson gson = new Gson();
    private final String kafka1_URL;

    TweetProducer(String kafka1_URL) {
        this.kafka1_URL = kafka1_URL;
        this.producer = createProducer();
        this.producer.initTransactions();
    }

    void produce(TweetValue tweetValue) {
        try {
            this.producer.beginTransaction();
            produceMentions(tweetValue);
            produceTags(tweetValue);
            produceLocation(tweetValue);
            this.producer.commitTransaction();
        } catch (ProducerFencedException e) {
            this.producer.close();
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            this.producer.abortTransaction();
            logger.error(e.getMessage());
            e.printStackTrace();
        }

    }

    private void produceLocation(TweetValue value) {
        logger.info("Publishing to topic " + getTopic(TweetFilter.LOCATION) + " the content " + value.toString());
        ProducerRecord<String, String> record = new ProducerRecord<>(getTopic(TweetFilter.LOCATION), value.getLocation(), gson.toJson(value));
        this.producer.send(record);
    }

    private void produceTags(final TweetValue tweetValue) {
        tweetValue.getTags().stream()
                .filter(m -> m.length() > 0)
                .map(key -> CustomPartitioner.partition(partitionsPerTopic(TweetFilter.TAG), key))
                .distinct()
                .peek(partition -> logger.info("Publishing to topic " + getTopic(TweetFilter.TAG) +
                        " partition " + partition + " the content " + tweetValue.toString()))
                .map(partition ->
                        new ProducerRecord<>(getTopic(TweetFilter.TAG), partition, "key", gson.toJson(tweetValue)))
                .forEach(record -> this.producer.send(record));
    }

    private void produceMentions(final TweetValue tweetValue) {
        tweetValue.getMentions().stream()
                .filter(m -> m.length() > 0)
                .map(key -> CustomPartitioner.partition(partitionsPerTopic(TweetFilter.MENTION), key))
                .distinct()
                .peek(partition -> logger.info("Publishing to topic " + getTopic(TweetFilter.MENTION) +
                        " partition " + partition + " the content " + tweetValue.toString()))
                .map(partition ->
                        new ProducerRecord<>(getTopic(TweetFilter.MENTION), partition, "key", gson.toJson(tweetValue)))
                .forEach(record -> this.producer.send(record));
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
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(props);
    }

    private String getTopic(TweetFilter tweetFilter) {
        return tweetFilter.toString().toLowerCase();
    }
}
