package it.polimi.middleware;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import it.polimi.middleware.model.TweetKey;
import it.polimi.middleware.model.TweetValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

class TweetConsumer {
    private static final Logger logger = Logger.getLogger(TweetConsumer.class);

    private String topic;
    private KafkaConsumer<TweetKey, TweetValue> consumer;

    TweetConsumer(String topic) {
        this.topic = topic;
        this.consumer = createConsumer();
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    ConsumerRecords<TweetKey, TweetValue> consume() {
        return consumer.poll(Duration.ofMillis(100));
    }

    void close() {
        this.consumer.close();
    }

    private KafkaConsumer<TweetKey, TweetValue> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consgroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        return new KafkaConsumer<>(props);
    }

    public static void main(String [] args) {
        TweetConsumer consumer = new TweetConsumer("tweets");
        while (true) {
            ConsumerRecords<TweetKey, TweetValue> records = consumer.consume();
            for (ConsumerRecord<TweetKey, TweetValue> record : records) {
                System.out.println("Key: " + record.key().getKey() + " - " + "Value:" + record.value().getContent());
            }
        }
    }
}
