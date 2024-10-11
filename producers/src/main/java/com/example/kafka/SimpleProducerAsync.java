package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerAsync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class);

    public static void main(String[] args) throws InterruptedException {
        String topicName = "simple-topic";
        // KafkaProducer configuration setting
        // null, "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "id-001", "아리갓또");

        // KafkaProducer message send
        kafkaProducer.send(producerRecord, ((metadata, exception) -> {
            if (exception == null) {
                logger.info("\n###### record metadata received ###### \n" +
                        "partition: {}\n" +
                        "offset: {}\n" +
                        "timestamp: {}", metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                logger.error("exception error from broker - {}", exception.getMessage());
            }
        }));
        Thread.sleep(1000);
        kafkaProducer.close();
    }
}
