package com.minsait.iclou.gbalastegui;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

public class BalanceComputer {
    public static void main(String[] args) {
        
        Initializer<String> balanceInitializer = String::new;

        Aggregator<String, String, String> balanceAggregator = (key, value, aggregate) -> {
            String newTotal;

            //we turn the value and aggregate strings into a json object so we can opperate
            JSONObject inputRegister = new JSONObject(value);
            JSONObject totalAggregate = new JSONObject(aggregate);

            //obtain the values we want to work with
            //ofc this can be done shorter directly in the newRegister definition
            String inputDate = inputRegister.getString("time");
            String totalDate = totalAggregate.getString("time");
            int inputAmount = inputRegister.getInt("amount");
            int accumulatedAmount = totalAggregate.getInt("amount");

            int newAmount = inputAmount + accumulatedAmount;
            
            //we take the newest date from the value and the aggregate
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-mm-dd'T'HH:mm:ss");
            LocalDateTime formatedInputDate = LocalDateTime.parse(inputDate, formatter);
            LocalDateTime formatedTotalDate = LocalDateTime.parse(totalDate, formatter);

            LocalDateTime formatedNewDate = (formatedInputDate.compareTo(formatedTotalDate) >=0) ? formatedInputDate : formatedTotalDate;
            
            String newDate = formatedNewDate.format(formatter);
            //we form the json object we want to return and put this into string format
            JSONObject newRegister = new JSONObject();
            newRegister.put("amount", newAmount);
            newRegister.put("time", newDate);
            
            newTotal = newRegister.toString();
            
            System.out.println("Procesado de sumatorio, devolviendo valor");
            return newTotal;
        };

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
            .mapValues(value -> {
                JSONObject balanceRegister = new JSONObject(value);
                balanceRegister.remove("Name");
                return balanceRegister.toString();
            })
            .peek((key, value) -> {
                System.out.println("Initial processing");
            });
        
        KTable <String, String> procesedBalance = balanceInitialProccess
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                balanceInitializer
                ,balanceAggregator
            );
        
        KStream<String, String> outputStream = procesedBalance.toStream();

        outputStream.to("AccountBalance", Produced.with(Serdes.String(), Serdes.String()));       

        KafkaStreams streams = new KafkaStreams(builder.build(), streamAppProperties);

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
