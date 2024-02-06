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
import org.apache.kafka.streams.kstream.Produced;

public class FavouriteColorPractice {
    public static void main(String[] args) {

        String[] availableValues = {"red", "green", "blue"};

        Properties properties = new Properties();

        
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "FavouriteColorCounter");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, "0"); //only in dev

        
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> colorCountInput = builder.stream("favouriteColor");

        KStream<String, String> userColorStream = colorCountInput
            .mapValues(message -> message.toLowerCase())
            .filter((key, value) -> value.contains(","))
            .filter((key, value) -> key == null)
            .filter((key, value) -> value != null)
            .selectKey((ignoredKey, value) -> value.substring(0,value.indexOf(",")))
            .mapValues((value) -> value.substring(value.indexOf(",")+1,value.length()))
            .filter((key,value) -> !value.contains(","))
            .filter((key, value) -> Arrays.asList(availableValues).contains(value));

        userColorStream.to("userColor");

        //KTable<String,String> userColorInputTable = builder.table("userColor");
        
        KTable<String, Long> userColorTable = userColorStream
            .groupBy((key, color) -> color)
            .count();
        
        KStream<String, Long> outputStream = userColorTable.toStream();

        outputStream.to("colorCountOutput", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp(); //only for dev
        streams.start();
        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
       


    }
}
