package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupV2 {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeupV2.class);

    public static void main(String[] args) {
        String topicName = "pizza-topic2";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-02");
//        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "3");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

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

        int lootCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ####### loopCnt:{}, consumerRecords Count: {}", lootCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Record Key: {}, Value: {}, Partition: {}",
                            record.key(), record.value(), record.partition());
                }

                try {
                    long sleepTime = lootCnt * 10000;
                    logger.info("Main Thread is Sleeping {} ms during while loop", sleepTime);
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
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