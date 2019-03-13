package com.epam.kafka;

import com.kafka.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;

public class SenderAvro {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<Long, Customer> kafkaProducer = new KafkaProducer(properties);

        Customer customer = Customer.newBuilder()
                .setFirstName("Piotr")
                .setLastName("Kapcia")
                .setAge(32)
                .setHeight(182.5f)
                .setWeight(86.3f)
                .setAutomatedEmail(true)
                .build();

        String topic = "customers";
        ProducerRecord<Long, Customer> producerRecord = new ProducerRecord<>(topic, customer);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println("Success");
                System.out.println(recordMetadata.toString());
            } else {
                e.printStackTrace();
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
