package com.github.simplesteph.udemy.kafka.streams;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class MyExtendedDataFetcher {

    public static void main(String[] args) {
        final Properties config = new Properties();

        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "MyConsumer");

        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(config);
             final Producer<String, String> producer = new KafkaProducer<>(config)) {
            consumer.subscribe(Collections.singletonList("basic-info"));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(1000);

                if (records.isEmpty()) {
                    continue;
                }

                records.forEach(consumerRecord -> {
                    final String key = consumerRecord.key();
                    producer.send(
                            new ProducerRecord<>("extended-info",
                                                 key,
                                                 String.format("Extended: (%s) %s",
                                                               key,
                                                               RandomStringUtils.randomAlphabetic(10))));
                });

                consumer.commitAsync();
            }
        }
    }
}
