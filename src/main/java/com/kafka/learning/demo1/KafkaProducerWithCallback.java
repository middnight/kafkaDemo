package com.kafka.learning.demo1;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerWithCallback {
    private static Logger LOGGER = LoggerFactory.getLogger(KafkaProducerWithCallback.class);

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
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        int i = 0;
        while (i < 100) {

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World" + i);

            //3. send the data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        LOGGER.info("New Message Produced :\n" +
                                "Topic Name : " + recordMetadata.topic() + "\n" +
                                "Partition " + recordMetadata.partition() + "\n" +
                                "Offset " + recordMetadata.offset());

                    } else {
                        LOGGER.error("Exception occured while producing the message. ", e);
                    }
                }
            });

            Thread.sleep(500);
            i++;

        }

        //flush data and close the producer
        producer.flush();
        producer.close();
    }

}
