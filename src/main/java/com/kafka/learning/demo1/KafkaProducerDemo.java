package com.kafka.learning.demo1;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {
    /*
     * 1. create producer properties
     * 2. create producer
     * 3. send data by that producer to kafka
     * */

    public static void main(String[] args) throws InterruptedException {

        //1. create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i=0;
        while (i<100) {
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", RandomStringUtils.random(20, true, true));
            //3. send the data
            producer.send(record);

            Thread.sleep(500);
            i++;

        }

        //flush data and close the producer
        producer.flush();
        producer.close();
    }

}