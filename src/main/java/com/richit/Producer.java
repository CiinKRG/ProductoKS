package com.richit;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    static Properties properties;
    public static void main(String[] args) {

        String filenameprops = args[0];
        GetPropertiesKafka getproperties = new GetPropertiesKafka();
        properties = getproperties.GetKafkaValues(filenameprops);

        Properties props = new Properties();

        String topic = properties.getProperty("topicNamep");

        props.put("bootstrap.servers", properties.getProperty("bootstrap.serversp"));
        props.put("client.id",properties.getProperty("client.idp"));
        props.put("acks",properties.getProperty("acksp"));
        props.put("retries", properties.getProperty("retriesp"));
        props.put("batch.size", properties.getProperty("batch.sizep"));
        props.put("linger.ms", properties.getProperty("linger.msp"));
        props.put("buffer.memory", properties.getProperty("buffer.memoryp"));
        props.put("key.serializer", properties.getProperty("key.serializerp"));
        props.put("value.serializer", properties.getProperty("value.serializerp"));

        System.out.println("After properties");
        //Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        //Create a producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,"A1","prueba producer");

        //Send data - asynchronous
        producer.send(record);

        //Flush data
        producer.flush();

        //Flush and close producer
        producer.close();
    }
}
