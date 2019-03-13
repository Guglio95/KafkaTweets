package it.polimi.middleware;

import com.google.gson.Gson;
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

    private void produce(String topic, Long key, TweetValue value) {
        logger.info("Publishing to topic "+topic.toLowerCase()+" the content "+value.toString());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic.toLowerCase(), key + "", gson.toJson(value));
        this.producer.send(record);
    }

    void enqueue(TweetValue tweetValue) {
        //Publish to mention queue
        tweetValue.getMentions().stream().filter(m -> m.length() > 0).forEach(mention -> produce("mention_" + mention, 1L, tweetValue));

        //Publish to tag queue
        tweetValue.getTags().stream().filter(m -> m.length() > 0).forEach(tag -> produce("tag_" + tag, 1L, tweetValue));

        //Publish to location queue
        produce("location_" + tweetValue.getLocation(), 1L, tweetValue);
    }

    void stop() {
        producer.flush();
        producer.close(1, TimeUnit.SECONDS);
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka1_URL);

//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
//        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(props);
    }
}
