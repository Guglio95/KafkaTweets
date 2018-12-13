package it.polimi.middleware;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import it.polimi.middleware.model.TweetDAO;
import it.polimi.middleware.model.TweetKey;
import it.polimi.middleware.model.TweetValue;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

class TweetConsumer {
    private static final Logger logger = Logger.getLogger(TweetConsumer.class);

    private String topic;
    private KafkaConsumer<TweetKey, TweetValue> consumer;
    private TweetDAO tweetDAO;

    TweetConsumer(String topic) {
        this.topic = topic;
        this.consumer = createConsumer();
        this.tweetDAO = new TweetDAO();
    }

    ConsumerRecords<TweetKey, TweetValue> consume() {
        return consumer.poll(Duration.ofMillis(100));
    }

    void start() {
        this.consumer.subscribe(Collections.singletonList(topic));
        long offset = tweetDAO.getOffset();
        consumer.poll(1);
        consumer.assignment().forEach(topicPartition -> consumer.seek(topicPartition, offset));
    }

    void stop() {
        this.consumer.close();
    }

    private KafkaConsumer<TweetKey, TweetValue> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consgroup1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");


        return new KafkaConsumer<>(props);
    }

    public static void main(String[] args) {
        TweetConsumer consumer = new TweetConsumer("tweets");
        consumer.start();
        while (true) {
            ConsumerRecords<TweetKey, TweetValue> records = consumer.consume();
            List<TweetValue> tweets = new ArrayList<>();

            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<TweetKey, TweetValue>> partitionRecords = records.records(partition);
                for (ConsumerRecord<TweetKey, TweetValue> record : partitionRecords) {
                    tweets.add(record.value());
                    System.out.println(partition.toString() + " " + record.offset() + ": " + record.value());
                }
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                consumer.tweetDAO.persists(tweets, lastOffset + 1);
                consumer.consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }
    }
}
