package com.github.simplesteph.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.Properties;

public class MyBankBalanceExactlyOnceApp {

    public static void main(String[] args) {
        final Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, JsonNode> bankTransactions =
                builder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonSerde));

        final ObjectNode initial = JsonNodeFactory.instance.objectNode();
        initial.put("count", 0);
        initial.put("balance", 0);
        initial.put("time", Instant.ofEpochMilli(0L).toString());

        final KTable<String, JsonNode> bankBalance = bankTransactions.groupByKey().aggregate(
                () -> initial,
                (key, transaction, currentBalance) -> newBalance(transaction, currentBalance),
                Materialized.as("bank-balance-agg").with(Serdes.String(), jsonSerde)
        );

        bankBalance.toStream().to("bank-balance-exactly-once", Produced.with(Serdes.String(), jsonSerde)); // make this topic log-compacted

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp();
        streams.start();

        System.out.println(topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode currentBalance) {
        final ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", currentBalance.get("count").asInt() + 1);
        newBalance.put("balance", currentBalance.get("balance").asInt() + transaction.get("amount").asInt());

        final Long candidateForLatest = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        final Long currentLatest = Instant.parse(currentBalance.get("time").asText()).toEpochMilli();
        final Instant newLatest = Instant.ofEpochMilli(Math.max(candidateForLatest, currentLatest));

        newBalance.put("time", newLatest.toString());

        return newBalance;
    }
}
