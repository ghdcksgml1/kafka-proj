package com.practice.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

public class BaseConsumer<K extends Serializable, V extends Serializable> {
    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topics;

    public BaseConsumer(Properties properties, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.topics = topics;

        initConsumer();
    }

    private void initConsumer() {
        this.kafkaConsumer.subscribe(topics);
        shutdownHookToRuntime();
    }

    private void shutdownHookToRuntime() {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info(" main program starts to exit by calling wakeup");
            this.kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        logger.info("record key: {}, partition: {}, record offset: {}, record value: {}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::processRecord);
    }

    public void pollConsumes(long durationMillis, String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            closeConsumer();
        }
    }

    private void closeConsumer() {
        this.kafkaConsumer.close();
    }

    private void pollCommitSync(long durationMillis) {
        ConsumerRecords<K, V> records = this.kafkaConsumer.poll(Duration.of(durationMillis, ChronoUnit.MILLIS));
        processRecords(records);
        this.kafkaConsumer.commitSync();
    }

    private void pollCommitAsync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> records = this.kafkaConsumer.poll(Duration.of(durationMillis, ChronoUnit.MILLIS));
        processRecords(records);
        this.kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                logger.error("offsets {} is not completed, error: {}", offsets, exception.getMessage());
            }
        });
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<>(properties, List.of(topicName));
        String commitMode = "sync";

        baseConsumer.pollConsumes(100, commitMode);
        baseConsumer.closeConsumer();
    }
}
