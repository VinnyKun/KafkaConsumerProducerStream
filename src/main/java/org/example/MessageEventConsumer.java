package org.example;

import org.example.model.Message;
import org.example.serdes.MessageDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Consumes state changes from Kafka and shows the updated data base on each state change event.
 */
public class MessageEventConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final String TOPIC = "messages-censored"; // change to messages-censored
    private final Map<String, Object> props;

    /**
     * Creates a new Kafka consumer.
     */
    public MessageEventConsumer() {
        props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "messages-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    /**
     * Returns a publisher that emits new events from Kafka.
     * @return A publisher that emits new events from Kafka.
     */
    public Flux<ReceiverRecord<String, Message>> consume(String username) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, username);
        final ReceiverOptions<String, Message> receiverOptions = ReceiverOptions.create(props);
        // Subscribe to users topic
        ReceiverOptions<String, Message> options = receiverOptions.subscription(Collections.singleton(TOPIC));
        // Create the publisher
        return KafkaReceiver.create(options).receive();
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter your recipient name to listen for messages: ");
        String receiverName = scanner.nextLine().trim();
        System.out.println("Listening for messages. Press Ctrl+C to exit.");

        // Initialize the Kafka consumer
        MessageEventConsumer consumer = new MessageEventConsumer();

        // Subscribe and listen for messages
        Flux<ReceiverRecord<String, Message>> events = consumer.consume(receiverName);

        events.filter(record -> record.value().getRecepientName().equals(receiverName))
                .doOnNext(record -> {
                    Message message = record.value();
                    System.out.println(message.getSenderName() + ": " + message.getBody());
                    record.receiverOffset().commit().subscribe();
                })
                .doOnError(error -> {
                    System.err.println("Error while consuming messages: " + error.getMessage());
                })
                .blockLast();
    }
}