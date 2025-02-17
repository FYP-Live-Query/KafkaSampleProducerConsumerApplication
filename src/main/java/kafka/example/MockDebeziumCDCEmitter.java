package kafka.example;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MockDebeziumCDCEmitter {
    private final static String SERVER_PLUS_PORT = "10.8.100.246:9092";
    private final static String EVENT_JSON_MSG_DIR = "src/main/resources/MockDebeziumResponse.json";
    private final static Logger LOGGER = Logger.getGlobal();
    private final static long RATE_LIMIT = 1000;

    private static void consumeMessageViaThisThread(){
        long threadId = Thread.currentThread().getId();

        // consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVER_PLUS_PORT);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,"mahesh-" + threadId);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG,"mahesh-thread-" + threadId);

        LOGGER.log(Level.INFO,"Thread " + threadId + " created. ");

        // creating consumer
        Consumer<UUID, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);

        // subscribe to topic
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        while(true) {
            ConsumerRecords<UUID, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<UUID, String> consumerRecord : consumerRecords) {
                LOGGER.log(Level.INFO,"consumer - " + threadId + " : " + consumerRecord.toString());
            }
            kafkaConsumer.commitSync(); //
        }
    }
    private static String loadJSONObjectFromFileIn() throws FileNotFoundException {
        String json = "";
        try {
            Gson gson = new Gson();
            Reader reader = Files.newBufferedReader(Paths.get(MockDebeziumCDCEmitter.EVENT_JSON_MSG_DIR));
            Map map = gson.fromJson(reader, Map.class);
            Object put = ((LinkedTreeMap) ((LinkedTreeMap) map.get("payload")).get("after")).put("eventTimestamp", System.currentTimeMillis());
            json = gson.toJson(map);
            reader.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return json;

    }

    private final static String TOPIC = "dbserver1.inventory.networkTraffic";
    public static void main(String[] args) throws ExecutionException, InterruptedException, FileNotFoundException {

        // properties
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,SERVER_PLUS_PORT); // if not working set listeners in kafka/config/server to listeners=PLAINTEXT://localhost:9092

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
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVER_PLUS_PORT);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // creating producer
        KafkaProducer<UUID, String> kafkaProducer = new KafkaProducer<>(producerProps);
        Runnable producerRunnable = () -> {
            while(true) {
                try {
                    String JSONStringMsg = loadJSONObjectFromFileIn();
                    ProducerRecord<UUID, String> producerRecord = new ProducerRecord<>(TOPIC,UUID.randomUUID(),JSONStringMsg);
                    kafkaProducer.send(producerRecord);
                    Thread.sleep(RATE_LIMIT); // msg rate limiter
                    LOGGER.log(Level.INFO, "msg :" + JSONStringMsg + " sent.");
                } catch (FileNotFoundException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable consumerRunnable = new Runnable() {
            @Override
            public void run() {
                consumeMessageViaThisThread();
            }
        };

        Thread producerThread = new Thread(producerRunnable);
        Thread consumerThread1 = new Thread(consumerRunnable);
        Thread consumerThread2 = new Thread(consumerRunnable);

        producerThread.start(); // creating producer thread
//        consumerThread1.start(); // creating consumer thread 1
//        consumerThread2.start(); // creating consumer thread 2

        producerThread.join();
//        consumerThread1.join();
//        consumerThread2.join();

    }
}
