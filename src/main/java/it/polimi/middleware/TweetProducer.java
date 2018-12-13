package it.polimi.middleware;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import it.polimi.middleware.model.TweetKey;
import it.polimi.middleware.model.TweetValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.TimeUnit;

class TweetProducer {

    private KafkaProducer<TweetKey, TweetValue> producer;

    TweetProducer() {
        this.producer = createProducer();
    }

    void produce(String topic, TweetKey key, TweetValue value) {
        ProducerRecord<TweetKey, TweetValue> record = new ProducerRecord<>(topic, key, value);
        this.producer.send(record);
    }

    void stop() {
        producer.flush();
        producer.close(1, TimeUnit.SECONDS);
    }

    private KafkaProducer<TweetKey, TweetValue> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(props);
    }
}
