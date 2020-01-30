package com.richit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    static Properties properties;
    public static void main(String[] args) {

        String filenameprops = args[0];
        GetPropertiesKafka getproperties = new GetPropertiesKafka();
        properties = getproperties.GetKafkaValues(filenameprops);

        Properties props = new Properties();

        String topic = properties.getProperty("topicNamec");
        String poll = properties.getProperty("pollc");

        props.put("bootstrap.servers", properties.getProperty("bootstrap.serversc"));
        props.put("group.id", properties.getProperty("group.idc"));
        props.put("enable.auto.commit", properties.getProperty("enable.commitc"));
        props.put("auto.commit.interval.ms", properties.getProperty("commit.intervalc"));
        props.put("client.id", properties.getProperty("client.idc"));
        props.put("auto.offset.reset",properties.getProperty("auto.offset.resetc"));
        props.put("key.deserializer", properties.getProperty("key.deserializerc"));
        props.put("value.deserializer",properties.getProperty("value.deserializerc"));

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //Subscribe Consumer to Topic(s)
        consumer.subscribe(Arrays.asList(topic));

        //Poll for new data
        /*while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(properties.getProperty("pollc")));
            //process(records); application-specific processing
            for (ConsumerRecord<String, String> record : records){
                System.out.println(record.value());
            }
        }*/

    }
}
