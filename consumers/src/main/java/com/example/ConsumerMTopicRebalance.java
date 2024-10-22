package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalance {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalance.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info(" main program starts to exit by calling wakeup.");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e ) {
                e.printStackTrace();
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("topic: {}, record key: {}, partition: {}, record offset:{}, record value: {}",
                            record.topic(), record.key(), record.partition(), record.offset(), record.value());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called.");
        } finally {
            logger.info("finally consumer is closeing");
            kafkaConsumer.close();
        }
    }
}