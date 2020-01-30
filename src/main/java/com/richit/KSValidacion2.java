package com.richit;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.log4j.Logger;
import org.json.CDL;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class KSValidacion2 {
    final static Logger logger = Logger.getLogger(KSValidacion2.class);
    static Properties properties;
    public static void main(String[] args) {
        String filenameprops = args[0];

        try{
            final Properties streamsConfig = new Properties();
            GetPropertiesKafka getproperties = new GetPropertiesKafka();
            properties = getproperties.GetKafkaValues(filenameprops);

            String header = properties.getProperty("headerks");
            String delim = properties.getProperty("delimks");

            int dh = StringUtils.countMatches(header, delim);
            System.out.println("Delimitador en header " + dh);
            final int[] dd = new int[0];
            final String[] data = new String[0];

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

            KStream<String,String>[] forks = inStream.branch(
                    (key, value) -> StringUtils.countMatches(value,delim) == dh,
                    (key, value) -> StringUtils.countMatches(value,delim) != dh
            );

            forks[0].map((key,value) -> {
                        String aux = header + "\n" + value;
                        JSONArray array = CDL.toJSONArray(aux);
                        String auxString = String.valueOf(array);
                        return new KeyValue<>(key,auxString);
                    }
            ).to(properties.getProperty("topic.out"),Produced.with(Serdes.String(),Serdes.String()));

            forks[1].map((key,value) -> {
                        String aux = value + ", No coincide el número de campos";
                        return new KeyValue<>(key,aux);
                    }
            ).to(properties.getProperty("topic.out.error"),Produced.with(Serdes.String(),Serdes.String()));

            final KafkaStreams streams = new KafkaStreams(topology,streamsConfig);
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        } catch (Exception e) {
            System.out.println("Entra en error");
            System.out.println(e.toString());
            logger.info(e.toString());
        }
    }
}
