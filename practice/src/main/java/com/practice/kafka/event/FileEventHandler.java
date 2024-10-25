package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {
    private static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);


    private final String topicName;
    private final KafkaProducer<String, String> kafkaProducer;
    private final boolean isSync;

    public FileEventHandler(String topicName, KafkaProducer<String, String> kafkaProducer, boolean isSync) {
        this.topicName = topicName;
        this.kafkaProducer = kafkaProducer;
        this.isSync = isSync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.getKey(), messageEvent.getValue());

        if (this.isSync) {
            RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
            logger.info("\n###### record metadata received ###### \n" +
                    "partition: {}\n" +
                    "offset: {}\n" +
                    "timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
            return;
        }

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
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        boolean sync = true;

        FileEventHandler fileEventHandler = new FileEventHandler(topicName, kafkaProducer, sync);
        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
        fileEventHandler.onMessage(messageEvent);
    }
}
