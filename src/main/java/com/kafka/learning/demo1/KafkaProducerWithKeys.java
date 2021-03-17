package com.kafka.learning.demo1;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerWithKeys {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerWithKeys.class);
    private static final Random random=new Random();
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

            String topic="first_topic";
            String message= RandomStringUtils.random(10,true,false);
            String key="key_"+random.nextInt(3);


            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,message);

            LOGGER.info("key is : "+key);

            //3. send the data
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    LOGGER.info("New Message Produced :\n" +
                            "Topic Name : " + recordMetadata.topic() + "\t" +
                            "Partition " + recordMetadata.partition() + "\t" +
                            "Offset " + recordMetadata.offset());

                } else {
                    LOGGER.error("Exception occurred while producing the message. ", e);
                }
            });

            Thread.sleep(2000);
            //i++;

        }

        //flush data and close the producer
        producer.flush();
        producer.close();
    }

}
