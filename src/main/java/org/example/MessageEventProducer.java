package org.example;

import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.Message;
import org.example.serdes.MessageSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.HashMap;
import java.util.Map;


/**
 * Produces user state change events to Kafka.
 */
public class MessageEventProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final String TOPIC = "messages-raw"; //change to messages-raw
    private final Logger log = LoggerFactory.getLogger(MessageEventProducer.class);
    private final KafkaSender<String, Message> sender;

    /**
     * Creates a new producer to Kafka
     * @param logLevel The desired logging level.
     */
    public MessageEventProducer(Level logLevel) {
        // configure Kafka
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "messages-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class);
        SenderOptions<String, Message> senderOptions = SenderOptions.create(props);
        sender = KafkaSender.create(senderOptions);

        // configure logger
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(logLevel);
        ctx.updateLoggers();
    }

    /**
     * Creates a publisher such that once subscribed, sends a message event to Kafka
     * @param m The user state change to produce to Kafka
     * @return The publisher such that once subscribed, sends the message event to Kafka.
     */
    public Mono<Void> send(Message m) {
        ProducerRecord<String, Message> producerRecord = new ProducerRecord<>(TOPIC, m.getSenderName(),m);
        SenderRecord<String, Message, Long> senderRecord = SenderRecord.create(producerRecord, System.currentTimeMillis());
        return this.sender.send(Mono.just(senderRecord)).then();
    }

    /**
     * Closes the producer.
     */
    public void close() {
        sender.close();
    }

}

