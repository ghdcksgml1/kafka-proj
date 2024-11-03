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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileToDBConsumer<K extends Serializable, V extends Serializable> {
    public static final Logger logger = LoggerFactory.getLogger(FileToDBConsumer.class);
    private final KafkaConsumer<K, V> kafkaConsumer;
    private final List<String> topics;
    private final OrderDBHandler orderDBHandler;

    public FileToDBConsumer(Properties properties,
                            List<String> topics, OrderDBHandler orderDBHandler) {
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.topics = topics;
        this.orderDBHandler = orderDBHandler;
    }

    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHook();
    }

    public void shutdownHook() {
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info(" main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private void processRecord(ConsumerRecord<K, V> record) throws Exception {
        OrderDto orderDto = makeOrderDto(record);
        orderDBHandler.insertOrder(orderDto);
    }

    private void processRecords(ConsumerRecords<K, V> records) throws Exception {
        List<OrderDto> orderDtoList = new ArrayList<>();
        for (var record : records) {
            orderDtoList.add(makeOrderDto(record));
        }
        orderDBHandler.insertOrders(orderDtoList);
    }

    private OrderDto makeOrderDto(ConsumerRecord<K, V> record) {
        String messageValue = (String) record.value();
        logger.info("###### messageValue: " + messageValue);
        String[] tokens = messageValue.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return new OrderDto(tokens[0], tokens[1], tokens[2], tokens[3],
                tokens[4], tokens[5], LocalDateTime.parse(tokens[6].trim(), formatter));
    }

    private void pollCommitAsync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> records = this.kafkaConsumer.poll(Duration.of(durationMillis, ChronoUnit.MILLIS));
                if (records.count() > 0) {
                    try {
                        processRecords(records);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                    this.kafkaConsumer.commitAsync(((offsets, exception) -> {
                        if (exception != null) {
                            logger.error("offsets {} is not completed, error: {}", offsets, exception.getMessage());
                        }
                    }));
                }
            }
        } catch (WakeupException e) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            this.kafkaConsumer.commitSync();
            close();
        }
    }

    private void close() {
        this.kafkaConsumer.close();
        this.orderDBHandler.close();
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        String url = "jdbc:postgresql://127.0.0.1:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        OrderDBHandler orderDBHandler = new OrderDBHandler(url, user, password);

        FileToDBConsumer<String, String> fileToDBConsumer = new FileToDBConsumer<>(props, List.of(topicName), orderDBHandler);
        fileToDBConsumer.initConsumer();
        String commitMode = "async";

        fileToDBConsumer.pollCommitAsync(1000);
        fileToDBConsumer.close();
    }
}
