package com.richit;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
    public static void main(String[] args) {

        String bootstrapServers = "localhost:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,"KSP1");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,"0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("C","A1","test2--------");

        //Send data - asynchronous
        producer.send(record);

        //Flush data
        producer.flush();

        //Flush and close producer
        producer.close();
    }
}
