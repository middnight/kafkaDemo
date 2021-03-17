package com.kafka.learning.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class KafkaConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "Server1";
        String topic="first_topic";

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);

        // subscribe consumer to the topics
        consumer.subscribe(Arrays.asList(topic));

        // poll the brokers for new data

        while (true){
            ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(500));

            for(ConsumerRecord<String,String> record:records){
                String processedString=record.value().toUpperCase(Locale.ROOT);

                logger.info("Topic : "+record.topic()+"\n"
                        +"Key : "+record.key()+"\n"+
                        "Message : "+record.value()+"\n"+
                        "Processed Message : "+processedString);
            }


        }


    }


}
