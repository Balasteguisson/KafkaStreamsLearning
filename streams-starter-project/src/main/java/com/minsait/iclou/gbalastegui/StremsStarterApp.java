package com.minsait.iclou.gbalastegui;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class StremsStarterApp {

    // Streams properties
    private static String StrAppIdConfig = "streams-starter-app";
    private static String StrBootstrapServer = "localhost:9092";
    private static String ConsOffSetResetConfig = "earliest";

    public static void main(String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StrAppIdConfig);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StrBootstrapServer);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsOffSetResetConfig);

        StreamsBuilder builder = new StreamsBuilder();

        // 1- Stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = wordCountInput
                // 2- map values to lower case
                .mapValues(textLine -> textLine.toLowerCase())

                // 3- flatmap values split by space
                .flatMapValues(lowerCasedText -> Arrays.asList(lowerCasedText.split(" ")))

                // 4- select key to apply a key
                .selectKey((ignoredKey, word) -> word)

                // 5- group by key
                .groupByKey()

                // 6- count occurrences
                .count(Materialized.as("Counts"));

        //6.2 convert KTable to KStream
        KStream<String, Long> wordCountStream = wordCounts.toStream();

        // 7- write back results to kafka
        wordCountStream.to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.cleanUp();
        streams.start();

        //print topology
        System.out.println(streams.toString());

        //graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
