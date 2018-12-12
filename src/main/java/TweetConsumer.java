import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import model.TweetKey;
import model.TweetValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class TweetConsumer {
    private void runConsumer() {
        Scanner in = new Scanner(System.in);

        KafkaConsumer<TweetKey, TweetValue> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList("tweet"));

        while(true) {
            String cmd = in.nextLine();
            switch (cmd) {
                case "read":
                    ConsumerRecords<TweetKey, TweetValue> records = consumer.poll(Duration.ofMillis(100));
                    records.iterator().forEachRemaining(tweet -> {
                        System.out.println(tweet.key() + " - " + tweet.value().getContent());
                    });
                    break;
                case "quit":
                    consumer.close();
                    in.close();
                    return;
                default:
                    System.out.println("Unknown command");
            }
        }
    }

    private KafkaConsumer<TweetKey, TweetValue> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consgroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new KafkaConsumer<>(props);
    }

    public static void main(String[] args) {
        try {
            TweetConsumer tweetConsumer = new TweetConsumer();
            tweetConsumer.runConsumer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
