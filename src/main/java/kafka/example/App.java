package kafka.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    private final static Logger LOGGER = Logger.getGlobal();
    private final static String TOPIC = "test-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // properties
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092"); // if not working set listeners in kafka/config/server to listeners=PLAINTEXT://localhost:9092

        // create kafka admin with given props
        Admin kafkaAdmin = KafkaAdminClient.create(adminProps);

        // get all topics
        ListTopicsResult listTopicsResult = kafkaAdmin.listTopics();
        listTopicsResult.listings().get().stream().forEach(topic -> LOGGER.log(Level.INFO,"topic name: " + topic.name() + ", topicID: " + topic.topicId()));

        boolean isTopicExists = listTopicsResult.listings().get().stream().anyMatch(topic -> topic.name().equals(TOPIC));

        // if topic does not exist create topics
        if(!isTopicExists) {
            NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 1);
            Collection<NewTopic> topicsToCreate = new ArrayList<>();
            topicsToCreate.add(newTopic);
            CreateTopicsResult createTopicsResult = kafkaAdmin.createTopics(topicsToCreate);
            LOGGER.log(Level.INFO,createTopicsResult.numPartitions(TOPIC).get().toString());
        } else {
            LOGGER.log(Level.INFO,"Topic is already created");
        }

        // creating producer thread

        // producer props
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // creating producer
        KafkaProducer<UUID, String> kafkaProducer = new KafkaProducer<>(producerProps);

        Runnable producerRunnable = new Runnable() {
            @Override
            public void run() {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("firstName", "John");
                jsonObject.put("lastName", "Smith");
                jsonObject.put("age", 25);
                jsonObject.put("address", "New York, New hampshire, Red Hack Road, USA.");
                ProducerRecord<UUID, String> producerRecord = new ProducerRecord<>(TOPIC,UUID.randomUUID(),jsonObject.toJSONString());
                while(true) {
                    try {
                        kafkaProducer.send(producerRecord);
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        // creating consumer thread

        // consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mahesh");

        // creating consumer
        Consumer<UUID, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);

        // subscribe to topic
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));
        Runnable consumerRunnable = new Runnable() {
            @Override
            public void run() {
                while(true) {
                    ConsumerRecords<UUID, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));
                    for (ConsumerRecord<UUID, String> consumerRecord : consumerRecords) {
                        System.out.println(consumerRecord.toString());
                    }
                }
            }
        };

        Thread producerThread = new Thread(producerRunnable);
        Thread consumerThread = new Thread(consumerRunnable);

        producerThread.start();
        consumerThread.start();

        producerThread.join();
        consumerThread.join();

    }
}
