package kafka.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App0 {
    private final static Logger LOGGER = Logger.getGlobal();
    private final static String TOPIC = "test-topic";
    public static void main(String[] args) throws InterruptedException {
        // consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mahesh-99");

        Runnable consumerRunnable = new Runnable() {
            @Override
            public void run() {
                long threadId = Thread.currentThread().getId();
                LOGGER.log(Level.INFO,"Thread " + threadId + " created. ");

                // creating consumer
                Consumer<UUID, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);

                // subscribe to topic
                kafkaConsumer.subscribe(Collections.singleton(TOPIC));

                while(true) {
                    ConsumerRecords<UUID, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<UUID, String> consumerRecord : consumerRecords) {
                        LOGGER.log(Level.INFO,"consumer - " + threadId + " : " + consumerRecord.toString());
                    }
                    kafkaConsumer.commitSync(); //
                }
            }
        };

        Thread consumerThread1 = new Thread(consumerRunnable);

        consumerThread1.start();

        consumerThread1.join();
    }
}
