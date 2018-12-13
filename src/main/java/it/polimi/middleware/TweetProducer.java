package it.polimi.middleware;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import it.polimi.middleware.model.TweetKey;
import it.polimi.middleware.model.TweetValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class TweetProducer {
    private static final Logger logger = Logger.getLogger(TweetProducer.class);

    private KafkaProducer<TweetKey, TweetValue> producer;

    TweetProducer() {
        this.producer = createProducer();
        this.producer.initTransactions();
    }

    void produce(String topic, TweetKey key, TweetValue value) {
        ProducerRecord<TweetKey, TweetValue> record = new ProducerRecord<>(topic, key, value);
        this.producer.send(record);
    }

    public void beginTransaction() {
        producer.beginTransaction();
    }

    public void commitTransaction() {
        producer.commitTransaction();
    }

    public void abortTransaction() {
        producer.abortTransaction();
    }

    void close() {
        producer.flush();
        producer.close(1, TimeUnit.SECONDS);
    }

    private KafkaProducer<TweetKey, TweetValue> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer<>(props);
    }

    public static void main(String [] args) {
        TweetProducer producer = new TweetProducer();

        try {
            producer.beginTransaction();
            for(int i = 0; i < 100; i++) {
                TweetKey key = new TweetKey(0);
                TweetValue value = new TweetValue(Integer.toString(i), 0, 0, "", Collections.emptyList(), Collections.emptyList());
                producer.produce("tweets", key, value);
            }
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            e.printStackTrace();
        } catch (KafkaException e) {
            e.printStackTrace();
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
