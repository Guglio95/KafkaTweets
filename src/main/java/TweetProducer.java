import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import model.TweetKey;
import model.TweetValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class TweetProducer {
    private void runProducer() {
        KafkaProducer<TweetKey, TweetValue> producer = createProducer();

        Scanner in = new Scanner(System.in);
        while(true) {
            TweetValue value = new TweetValue();
            TweetKey key = new TweetKey(0);
            System.out.println("New Tweet");
            System.out.print("content: ");
            value.setContent(in.nextLine());

            ProducerRecord<TweetKey, TweetValue> record = new ProducerRecord<>("tweet", key, value);
            producer.send(record);
            System.out.println("Tweet sent.");
        }

    }

    private KafkaProducer<TweetKey, TweetValue> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer<>(props);
    }


    public static void main(String[] args) {
        try {
            TweetProducer tweetProducer = new TweetProducer();
            tweetProducer.runProducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
