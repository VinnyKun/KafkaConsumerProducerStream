package org.example;

import org.apache.logging.log4j.Level;
import org.example.model.Message;
import java.util.Scanner;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

public class Main {
    private final static String SENDER = "sender";
    private final static String RECEIVER = "receiver";

    private final static String MONITOR = "monitor";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Are you a sender or a receiver? (Type 'sender' or 'receiver')");
        String role = scanner.nextLine().trim().toLowerCase();

        if (SENDER.equals(role)) {
            System.out.println("You've selected sender mode.");
            System.out.print("Enter your sender name: ");
            String senderName = scanner.nextLine().trim();

            // Initialize the Kafka producer
            MessageEventProducer producer = new MessageEventProducer(Level.OFF);

            // Main loop to send messages
            while (true) {
                System.out.print("Enter receiver name: ");
                String receiverName = scanner.nextLine().trim();

                System.out.print("Enter message body: ");
                String body = scanner.nextLine().trim();

                // Create and send the message
                Message message = new Message(senderName, receiverName, body);
                producer.send(message).subscribe();

                System.out.println("Message sent to " + receiverName + ". Type 'exit' to quit or press Enter to continue sending.");
                String exit = scanner.nextLine().trim();
                if ("exit".equalsIgnoreCase(exit)) {
                    break;
                }
            }

            // Cleanup
            producer.close();
            System.out.println("Exiting sender mode.");
        } else if (RECEIVER.equals(role)) {
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
        } else if (MONITOR.equals(role)) {
            System.out.print("Enter your monitor name to listen for messages: ");
            String monitorName = scanner.nextLine().trim();
            System.out.println("Listening for messages. Press Ctrl+C to exit.");
            MessageCountConsumer consumer = new MessageCountConsumer();

            // Subscribe and listen for messages
            Flux<ReceiverRecord<String, Long>> events = consumer.consume(monitorName);

            events.doOnNext(record -> {
                        Long count = record.value();
                        System.out.println(record.key()+ " has send a grand total of : " + count + " messages sent!");
                        record.receiverOffset().commit().subscribe();
                    })
                    .doOnError(error -> {
                        System.err.println("Error while consuming messages: " + error.getMessage());
                    })
                    .blockLast();

        }else {
            System.out.println("Invalid role selected. Please restart the application and select either 'sender' or 'receiver'.");
        }
    }
}
