package com.richit;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.CDL;
import org.json.JSONArray;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static com.richit.GetValidations.*;

public class KSValidacion {
    static Properties properties;
    public static void main(String[] args) {

        String filenameprops = args[0];
        GetPropertiesKafka getproperties = new GetPropertiesKafka();
        properties = getproperties.GetKafkaValues(filenameprops);

        Properties propsC = new Properties(); //Propiedades consumer
        Properties propsP = new Properties(); //Propiedades producer

        String topicC = properties.getProperty("topicNamec"); //Topic consumer
        String topicP = properties.getProperty("topicNamep"); //Topic Producer
        String topicPE = properties.getProperty("topicNameError"); //Topic Producer Error
        String poll = properties.getProperty("pollc");
        String header = properties.getProperty("header");

        propsC.put("bootstrap.servers", properties.getProperty("bootstrap.serversc"));
        propsC.put("group.id", properties.getProperty("group.idc"));
        propsC.put("enable.auto.commit", properties.getProperty("enable.commitc"));
        propsC.put("auto.commit.interval.ms", properties.getProperty("commit.intervalc"));
        propsC.put("client.id", properties.getProperty("client.idc"));
        propsC.put("auto.offset.reset",properties.getProperty("auto.offset.resetc"));
        propsC.put("key.deserializer", properties.getProperty("key.deserializerc"));
        propsC.put("value.deserializer",properties.getProperty("value.deserializerc"));
        propsP.put("bootstrap.servers", properties.getProperty("bootstrap.serversp"));
        propsP.put("client.id",properties.getProperty("client.idp"));
        propsP.put("acks",properties.getProperty("acksp"));
        propsP.put("retries", properties.getProperty("retriesp"));
        propsP.put("batch.size", properties.getProperty("batch.sizep"));
        propsP.put("linger.ms", properties.getProperty("linger.msp"));
        propsP.put("buffer.memory", properties.getProperty("buffer.memoryp"));
        propsP.put("key.serializer", properties.getProperty("key.serializerp"));
        propsP.put("value.serializer", properties.getProperty("value.serializerp"));


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(propsC); //Create Consumer
        consumer.subscribe(Arrays.asList(topicC)); //Subscribe Consumer to Topic(s)

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(propsP); //Create Producer
        //ProducerRecord<String,String> recordP;

        String dh = FindDelimH(header);
        String data,dd;
        int fields = StringUtils.countMatches(header, dh); //Cuantas veces el delimitador se encuentra en el header

        //Poll for new data
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(poll)));
            for (ConsumerRecord<String, String> record : records){
                data = record.value();
                dd = FindDelimD(data, fields);
                //Convierte a JSON si coincide
                if (dd == "1") { //Envio a topico de error
                    //recordP = new ProducerRecord<String, String>(topicPE,record.key(),data + ", Menos campos vs header");
                    producer.send(new ProducerRecord<String, String>(topicPE,record.key(),data + ", Menos campos vs header"));
                    //producer.flush();
                    producer.close();
                }
                else if (dd == "2") { //Envio a topico de error
                    //recordP = new ProducerRecord<String, String>(topicPE,record.key(),data + ", Mas campos vs header");
                    producer.send(new ProducerRecord<String, String>(topicPE,record.key(),data + ", Mas campos vs header"));
                    //producer.flush();
                    producer.close();
                }
                else {
                    if (fields == 0) { //Toma el caso de un campo
                        String fc = header + "\n" + data;
                        JSONArray array = CDL.toJSONArray(fc);
                        System.out.println(array.toString(2));
                        //recordP = new ProducerRecord<String, String>(topicP,record.key(),data);
                        //producer.send(recordP);
                        //producer.flush();
                        //producer.close();
                    } else {
                        //Cambia el delimitador del header por comas
                        if (dh!=",") header=header.replace(dh,",");
                        if (dd != ",") data = data.replace(dd, ",");
                        String fc = header + "\n" + data;
                        JSONArray array = CDL.toJSONArray(fc);
                        System.out.println(array.toString(2));
                        //recordP = new ProducerRecord<String, String>(topicP,record.key(),data);
                        //producer.send(recordP);
                        //producer.flush();
                        //producer.close();
                    }
                }
            }
        }
    }
}
