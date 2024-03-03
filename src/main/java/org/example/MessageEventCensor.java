package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KafkaStreams;
import org.example.model.Message;
import org.example.serdes.MessageDeserializer;
import org.example.serdes.MessageSerializer;

import java.util.Properties;
import java.util.regex.Pattern;

public class MessageEventCensor {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final String SOURCE_TOPIC = "messages-raw"; // same as producer or last stream
    private static final String SINK_TOPIC = "messages-censored"; // same as consumer
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

        source.peek((k, v) -> System.out.printf("key: %s -> value: %s\n", k, v.toString()))
                .mapValues((k, v) -> {
                    String censoredBody = censorText(v.getBody());
                    v.setBody(censoredBody);
                    return v;
                })
                .peek((k, v) -> System.out.printf("key: %s -> new value: %s\n", k, v.toString()))
                .to(SINK_TOPIC, Produced.with(Serdes.String(), MessageSerde));

        KafkaStreams s = new KafkaStreams(builder.build(), p);
        s.start();
    }

    private static String censorText(String text) {
        Pattern patternFuck = Pattern.compile("fuck", Pattern.CASE_INSENSITIVE);
        Pattern patternShit = Pattern.compile("shit", Pattern.CASE_INSENSITIVE);
        Pattern patternBitch = Pattern.compile("bitch", Pattern.CASE_INSENSITIVE);
        text = patternFuck.matcher(text).replaceAll("f**k");
        text = patternShit.matcher(text).replaceAll("s**t");
        text = patternBitch.matcher(text).replaceAll("b***h");
        return text;
    }
}
