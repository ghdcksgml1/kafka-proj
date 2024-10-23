package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerPartitionAssignSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssignSeek.class);

    public static void main(String[] args) {
        String topicName = "pizza-topic-ex";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-pizza-assign-seek-v001");
//        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
//        kafkaConsumer.subscribe(List.of(topicName));
        kafkaConsumer.assign(List.of(topicPartition));
        kafkaConsumer.seek(topicPartition, 10L);

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

//        pollAutoCommit(kafkaConsumer);
//        pollCommitSync(kafkaConsumer);
//        pollCommitAsync(kafkaConsumer);
        pollNoCommit(kafkaConsumer);
    }

    private static void pollNoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int lootCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                logger.info(" ####### loopCnt:{}, consumerRecords Count: {}", lootCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Record Key: {}, Value: {}, record offset: {}, Partition: {}",
                            record.key(), record.value(), record.offset(), record.partition());
                }

            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called.");
        } finally {
            logger.info("finally consumer is closeing");
            kafkaConsumer.close();
        }
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int lootCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                logger.info(" ####### loopCnt:{}, consumerRecords Count: {}", lootCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Record Key: {}, Value: {}, record offset: {}, Partition: {}",
                            record.key(), record.value(), record.offset(), record.partition());
                }

                kafkaConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        logger.error("offsets {} is not completed, error: {}", offsets, exception);
                    } else {
                        logger.info("Thread Name: {}", Thread.currentThread().getName());
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called.");
        } finally {
            logger.info("finally consumer is closeing");
            kafkaConsumer.commitSync();
            kafkaConsumer.close();
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int lootCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ####### loopCnt:{}, consumerRecords Count: {}", lootCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Record Key: {}, Value: {}, record offset: {}, Partition: {}",
                            record.key(), record.value(), record.offset(), record.partition());
                }

                if (!consumerRecords.isEmpty()) {
                    try {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    } catch (CommitFailedException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called.");
        } finally {
            logger.info("finally consumer is closeing");
            kafkaConsumer.close();
        }
    }

    public static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int lootCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ####### loopCnt:{}, consumerRecords Count: {}", lootCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Record Key: {}, Value: {}, record offset: {}, Partition: {}",
                            record.key(), record.value(), record.offset(), record.partition());
                }

                try {
                    logger.info("Main Thread is Sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
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