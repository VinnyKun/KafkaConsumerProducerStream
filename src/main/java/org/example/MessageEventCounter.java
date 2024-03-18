package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KafkaStreams;
import org.example.model.Message;
import org.example.serdes.MessageDeserializer;
import org.example.serdes.MessageSerializer;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * This class is not used. Was built for practice. Flink Version is used instead for project
 */
public class MessageEventCounter {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
//    private static final String SOURCE_TOPIC = "messages-raw"; // same as producer or last stream
//    private static final String SINK_TOPIC = "messages-censored"; // same as consumer

    private static final String SOURCE_TOPIC = "messages-censored";
    private static final String SINK_TOPIC = "messages-counted";

    private static final String STATE_STORE_NAME = "message-counts-by-user";
    public static Properties getKafkaStreamsConfiguration(String applicationId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.DoubleSerde.class);
        return p;
    }

    public static void main(String[] args) {
        Properties p = getKafkaStreamsConfiguration("message-event-censor");
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Message> MessageSerde = Serdes.serdeFrom(new MessageSerializer(), new MessageDeserializer());
        KStream<String, Message> source = builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), MessageSerde));
        KTable<String, Long> messageCountByUser = source
                .groupByKey()
                .count(Materialized.as(STATE_STORE_NAME));

        source.peek((k, v) -> System.out.printf("key: %s -> value: %s\n", k, v.toString()));

        messageCountByUser.toStream().to(SINK_TOPIC,Produced.with(Serdes.String(), Serdes.Long()));
//                .groupByKey()
//                .count(Materialized.as(STATE_STORE_NAME));

//                .peek((k, v) -> System.out.printf("key: %s -> new value: %s\n", k, v.toString()))
//                .to(SINK_TOPIC, Produced.with(Serdes.String(), MessageSerde));

        try {
            KafkaStreams s = new KafkaStreams(builder.build(), p);
            s.start();
            while (!s.state().equals(KafkaStreams.State.RUNNING)) {
                try {
                    System.out.println("Waiting for Kafka Streams to enter RUNNING state...");
                    Thread.sleep(1000); // Check every second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Interrupted while waiting for Kafka Streams state.");
                    break;
                }
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }

    }
}
