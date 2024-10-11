package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCB {
    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCB.class);

    public static void main(String[] args) throws InterruptedException {
        String topicName = "multipart-topic";
        // KafkaProducer configuration setting
        // null, "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {

            // ProducerRecord 객체 생성
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "아리갓또" + seq);
            Callback callback = new CustomCallback(seq);

            // KafkaProducer message send
            kafkaProducer.send(producerRecord, callback);
        }
        Thread.sleep(1000);
        kafkaProducer.close();
    }
}
