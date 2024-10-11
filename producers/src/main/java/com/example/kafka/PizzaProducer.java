package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {
    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);
    public static CountDownLatch countDownLatch;

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer, String topicName, int iterCount,
                                        int interIntervalMillis, int intervalMillis, int intervalCount, boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();

        int iterSeq = 0;

        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq++ != iterCount) {
            HashMap<String, String> pizzaOrder = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pizzaOrder.get("key"), pizzaOrder.get("message"));

            sendMessage(kafkaProducer, producerRecord, sync);
            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("###### Interval Count: {}, Interval Millis: {} ######", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("###### Inter Interval Millis: {} ######", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public static void sendMessage(
            KafkaProducer<String, String> kafkaProducer,
            ProducerRecord<String, String> producerRecord,
            boolean sync
    ) {
        if (sync) {
            try {
                RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message: {}, partition: {}, offset: {}", producerRecord.value(), recordMetadata.partition(), recordMetadata.offset());
                countDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("async message: {}, partition: {}, offset: {}", producerRecord.value(), metadata.partition(), metadata.offset());
            } else {
                logger.error("exception error from broker {}", exception.getMessage());
            }
            countDownLatch.countDown();
        });
    }

    public static void main(String[] args) throws InterruptedException {
        String topicName = "pizza-topic";
        // KafkaProducer configuration setting
        // null, "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        int iterCount = -1;
        countDownLatch = new CountDownLatch(Integer.MAX_VALUE);
        new Thread(() -> {
            sendPizzaMessage(kafkaProducer, topicName, iterCount, 10, 100, 100, true);
        }).start();

        countDownLatch.await();
        kafkaProducer.close();
    }
}
