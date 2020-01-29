package com.richit;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import static com.richit.GetValidations.*;

public class KSValidacion2 {
    final static Logger logger = Logger.getLogger(KSValidacion2.class);
    static Properties properties;
    public static void main(String[] args) {
        String filenameprops = args[0];

        try{
            final Properties streamsConfig = new Properties();
            GetPropertiesKafka getproperties = new GetPropertiesKafka();
            properties = getproperties.GetKafkaValues(filenameprops);

            int dh = StringUtils.countMatches(properties.getProperty("header"), properties.getProperty("delim"));
            int dd;
            String data;

            streamsConfig.put("group.id",properties.getProperty("group.idk"));
            streamsConfig.put("application.id", properties.getProperty("app.idk"));
            streamsConfig.put("client.id", properties.getProperty("client.idk"));
            ArrayList bootstrapservers= new ArrayList(Arrays.asList(properties.getProperty("bootstrap.serversk").split(",")));
            streamsConfig.put("bootstrap.servers",bootstrapservers );
            streamsConfig.put("acks",properties.getProperty("acksk"));
            streamsConfig.put("key.serializer",properties.getProperty("key.serializerk"));
            streamsConfig.put("value.serializer",properties.getProperty("value.serializerk"));
            streamsConfig.put("auto.offset.reset",properties.getProperty("auto.offset.resetk"));
            streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,WallclockTimestampExtractor.class.getName());
            streamsConfig.put("max.request.size", properties.getProperty("max.request.sizek"));

            logger.info("Propiedades cargadas");
            final StreamsBuilder builder = new StreamsBuilder();
            final Topology topology = builder.build();

            final KStream<String,String> inStream = builder.stream(properties.getProperty("topic.in"), Consumed.with(Serdes.String(),Serdes.String()));

            KStream<String,String> outstring = null;
            //Proceso

            outstring.to(properties.getProperty("topic.out"),Produced.with(Serdes.String(),Serdes.String()));

            final KafkaStreams streams = new KafkaStreams(topology,streamsConfig);
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        } catch (Exception e) {
            System.out.println(e.toString());
            logger.info(e.toString());
        }
        /*String filenameprops = args[0];
        GetPropertiesKafka getproperties = new GetPropertiesKafka();
        properties = getproperties.GetKafkaValues(filenameprops);

        Properties propsC = new Properties(); //Propiedades consumer
        Properties propsP = new Properties(); //Propiedades producer

        String topicC = propertiesKS.getProperty("topicNamec"); //Topic consumer
        String topicP = propertiesKS.getProperty("topicNamep"); //Topic Producer
        String topicPE = propertiesKS.getProperty("topicNameError"); //Topic Producer Error
        String poll = propertiesKS.getProperty("pollc");
        String header = propertiesKS.getProperty("header");
        String delim = propertiesKS.getProperty("delimitador");

        propsC.put("bootstrap.servers", propertiesKS.getProperty("bootstrap.serversc"));
        propsC.put("group.id", propertiesKS.getProperty("group.idc"));
        propsC.put("enable.auto.commit", propertiesKS.getProperty("enable.commitc"));
        propsC.put("auto.commit.interval.ms", propertiesKS.getProperty("commit.intervalc"));
        propsC.put("client.id", propertiesKS.getProperty("client.idc"));
        propsC.put("auto.offset.reset",propertiesKS.getProperty("auto.offset.resetc"));
        propsC.put("key.deserializer", propertiesKS.getProperty("key.deserializerc"));
        propsC.put("value.deserializer",propertiesKS.getProperty("value.deserializerc"));
        propsP.put("bootstrap.servers", propertiesKS.getProperty("bootstrap.serversp"));
        propsP.put("client.id",propertiesKS.getProperty("client.idp"));
        propsP.put("acks",propertiesKS.getProperty("acksp"));
        propsP.put("retries", propertiesKS.getProperty("retriesp"));
        propsP.put("batch.size", propertiesKS.getProperty("batch.sizep"));
        propsP.put("linger.ms", propertiesKS.getProperty("linger.msp"));
        propsP.put("buffer.memory", propertiesKS.getProperty("buffer.memoryp"));
        propsP.put("key.serializer", propertiesKS.getProperty("key.serializerp"));
        propsP.put("value.serializer", propertiesKS.getProperty("value.serializerp"));


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(propsC); //Create Consumer
        consumer.subscribe(Arrays.asList(topicC)); //Subscribe Consumer to Topic(s)

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(propsP); //Create Producer

        int dh = StringUtils.countMatches(header, delim);
        int dd;
        String data;

        //Poll for new data
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(poll)));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Record.value = " + record.value());
                data = record.value();
                dd = StringUtils.countMatches(data, delim);

                if (dd == dh) {
                    System.out.println("Coincide");
                } else {
                    System.out.println("No coincide");
                }
            }
        }*/
    }
}
