package com.minsait.iclou.gbalastegui;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalanceProducer {

    private static final String recordsTopic = "BalanceMovements";
    

    private static final Logger log = LoggerFactory.getLogger(BalanceProducer.class);

    public static void main(String[] args) {
        log.info("Creating balance account registers.");

        
        //Settings section
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Producer creation
        KafkaProducer<String, String> balanceProducer = new KafkaProducer<>(producerProperties);

        //loop to generate 10 records per second
        for (int i = 0; i < 7; i++) {
            ProducerRecord<String, String> balanceRecord = new ProducerRecord<String,String>(recordsTopic, createBalanceRegister());     
            try {
                balanceProducer.send(balanceRecord);
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        balanceProducer.flush();
        balanceProducer.close();
    }

    //function that creates the json object with the balance register
    private static String createBalanceRegister (){
        String[] customers = {"Guille", "Dani", "Varo", "Morgan", "Costas", "Atre"};
        
        Random random = new Random();
        int randomCust = random.nextInt(6);
        int balance = random.nextInt(300);

        LocalDateTime transactionDate = LocalDateTime.now();

        JSONObject balanceRegister = new JSONObject();
        balanceRegister.put("Name", customers[randomCust]);
        balanceRegister.put("amount", balance);
        balanceRegister.put("time", transactionDate);

        return balanceRegister.toString();


    }
}
