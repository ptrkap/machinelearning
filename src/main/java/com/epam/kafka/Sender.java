package com.epam.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Sender {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer kafkaProducer = new KafkaProducer(properties);

        ProducerRecord record = new ProducerRecord<Long, String>("foobar", "test23");
        RecordMetadata meta = (RecordMetadata) kafkaProducer.send(record).get();
        System.out.println("partition: " + meta.partition());
        System.out.println("offset: " + meta.offset());
    }
}
