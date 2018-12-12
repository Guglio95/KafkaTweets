package it.polimi.middleware.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class Producer {
    private static final Logger logger = Logger.getLogger(Producer.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public Producer(String serverURL, String transactionalId, String topic) {
        logger.info("Creating produer");
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverURL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        producer = new KafkaProducer<>(props);

        producer.initTransactions();
    }

    public void close() {
        logger.info("Closing producer.");
        producer.flush();
        producer.close(1, TimeUnit.SECONDS);
    }

    public void enqueue(String key, String tweet) {
        logger.info("Sending " + tweet.toString());
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, tweet);
        producer.beginTransaction();
        producer.send(record);
        producer.commitTransaction();
    }
}