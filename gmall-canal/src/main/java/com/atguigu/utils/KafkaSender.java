package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Skipper
 * @date 2020/08/18
 * @desc
 */
public class KafkaSender {
    private KafkaSender() {
    }

    private static KafkaProducer<String,String> kafkaProducer = null;

    public static KafkaProducer<String,String> createKafkaSender(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }

    public static void sendToKafka(String topic,String message){
        if (kafkaProducer == null){
            kafkaProducer = createKafkaSender();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic,message));
    }
}
