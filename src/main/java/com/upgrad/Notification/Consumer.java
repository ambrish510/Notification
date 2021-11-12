package com.upgrad.Notification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class Consumer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","ec2-user@ec2-18-211-51-189.compute-1.amazonaws.com:9092");
        properties.setProperty("group.id","test");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");



        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("chat_message"));
        Set<String> SubScribedTopics = kafkaConsumer.subscription();
        SubScribedTopics.stream().forEach(System.out::println);

        try{
            while(true){
                ConsumerRecords<String,String> consumerRecords =  kafkaConsumer.poll(Duration.ofMinutes(2));
                for (ConsumerRecord<String, String> records: consumerRecords) {
                    System.out.printf("offset = %d, key = %s , vlaue = %s%n", records.offset(),records.key(),records.value());
                }
            }
        }finally {
            kafkaConsumer.close();
        }

    }
}
