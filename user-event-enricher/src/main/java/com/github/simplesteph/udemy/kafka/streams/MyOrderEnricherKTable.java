package com.github.simplesteph.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class MyOrderEnricherKTable {

    public static void main(String[] args) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-enricher-app-ktables");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> extendedInfo = builder.table("extended-info-ktable");
        final KTable<String, String> basicInfo = builder.table("basic-info");

        final KTable<String, String> enriched = extendedInfo.join(basicInfo,
                (extended, basic) -> String.format("ENRICHED: {%s -- %s}", basic, extended));

        enriched.toStream().to("enriched-info-ktables");


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        System.out.println(topology.describe());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
