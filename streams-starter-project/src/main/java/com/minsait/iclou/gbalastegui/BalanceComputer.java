package com.minsait.iclou.gbalastegui;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;

public class BalanceComputer {
    public static void main(String[] args) {
        Properties streamAppProperties = new Properties();

        streamAppProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "BalanceComputer");
        streamAppProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamAppProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamAppProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamAppProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamAppProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> balanceInput = builder.stream("BalanceMovements");

        KStream<String, String> balanceInitialProccess = balanceInput
            .selectKey((key, value) -> {
                JSONObject balanceRegister = new JSONObject(value);
                return balanceRegister.getString("Name");
            })
            .peek((key,value) -> {
                System.out.println(value);
            });
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamAppProperties);

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        
    }
}
