package com.github.simplesteph.udemy.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyOrderDataProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        try (final Producer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(basicInfoRecord("1", "Basic One")).get();
            producer.send(extendedInfoRecord("1", "Extended One")).get();

            producer.send(basicInfoRecord("2", "Basic Two")).get();
            producer.send(basicInfoRecord("3", "Basic Three")).get();
            producer.send(extendedInfoRecord("2", "Extended Two")).get();

            producer.send(basicInfoRecord("3", "Basic Three Three")).get();
            producer.send(basicInfoRecord("1", "Basic One One")).get();

            producer.send(extendedInfoRecord("3", "Extended Three")).get();
            producer.send(extendedInfoRecord("1", "Extended One One")).get();
        }


    }

    private static ProducerRecord<String, String> basicInfoRecord(String key, String value) {
        return new ProducerRecord<>("basic-info", key, value);
    }


    private static ProducerRecord<String, String> extendedInfoRecord(String key, String value) {
        return new ProducerRecord<>("extended-info-ktable", key, value);
    }
}
