package com.upgrad.Notification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class Consumer {

    public static void main(String[] args) {

        /*
        Setting up the properties for the consumer
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ec2-user@ec2-18-211-51-189.compute-1.amazonaws.com:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /*
        Subscribing to the kafka topic & reading message from kafka topic
         */
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("booking_confirmation_message"));
        Set<String> SubScribedTopics = kafkaConsumer.subscription();
        SubScribedTopics.forEach(System.out::println);

        /*
        Printing the message from kafka topic
         */
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMinutes(2));
                for (ConsumerRecord<String, String> records : consumerRecords) {
                    System.out.printf("offset = %d, key = %s , value = %s%n", records.offset(), records.key(), records.value());
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
