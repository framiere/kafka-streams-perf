package com.github.framiere;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.lang.Runtime.*;
import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.apache.kafka.common.serialization.Serdes.String;

/*
./confluent destroy
./confluent start kafka
./kafka-topics --zookeeper localhost:2181 --create --topic input --partitions 6 --replication-factor 1
./kafka-topics --zookeeper localhost:2181 --create --topic output --partitions 6 --replication-factor 1
./kafka-topics --zookeeper localhost:2181 --list
./mvn install
java -classpath target/streams-*jar com.github.framiere.ProduceDummyEvents
java -classpath target/streams-*jar com.github.framiere.Reduce
./kafka-console-consumer --bootstrap-server localhost:9092 --topic output
./kafka-console-consumer --bootstrap-server localhost:9092 --topic input
 */
public class Reduce {
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new AfterburnerModule());
    private static final Pattern ACTIONS = Pattern.compile("CONNECTION|DISCONNECTION|UP|DOWN");
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.getDefault());
    private static Stopwatch watch;

    public static void main(final String[] args) {
        Properties props = buildProperties();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("input");
        stream.mapValues(Reduce::jsonToEvent)
                .filter((key, value) -> ACTIONS.matcher(value.action).matches())
                .mapValues(Reduce::toTimedChannelUIDActionPair)
                .groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(1)))
                .reduce((a, b) -> a.substring(0, 13).compareTo(b.substring(0, 13)) > 0 ? a : b)
                .mapValues((key, value) -> toOutput(key, value))
                .toStream()
                .to("output", Produced.with(buildWindowedSerde(), String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        watch = Stopwatch.createStarted();
        getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String toOutput(Windowed<String> key, String value) {
        String[] split = value.split("\\|");
        String canal = split[1];
        String action = split[2];

        if (!"CONNECTION".equals(action)) {
            canal = "-1";
        }
        String substring = key.toString().substring(1, 15);

        try {
            return objectMapper.writeValueAsString(new Output(substring, canal));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties buildProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reducer-" + randomAlphanumeric(5));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, getRuntime().availableProcessors());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        return props;
    }

    private static int nbEvents = 0;

    private static Event jsonToEvent(String json) {
        try {
            if (++nbEvents % 100_000 == 0) {
                long elapsedInS = watch.elapsed(TimeUnit.SECONDS);
                if (elapsedInS > 0) {
                    System.out.println("Processed " + NUMBER_FORMAT.format(nbEvents) + " events (" + NUMBER_FORMAT.format(nbEvents / elapsedInS) + " events/s)");
                }
            }
            return objectMapper.readValue(json, Event.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static String toTimedChannelUIDActionPair(String key, Event value) {
        long timestamp = value.timeStamp;
        if ("CONNECTION".equals(value.action)) {
            timestamp -= 59000;
        }
        return timestamp + "|" + value.channelUID + "|" + value.action;
    }

    public static class MyTimestampExtractor implements TimestampExtractor {
        private long assignedTime = System.currentTimeMillis();
        private int current = 0;
        private final int max = 15_000;

        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousTimestamp) {
            if (current++ > max) {
                assignedTime = System.currentTimeMillis();
                current = 0;
            }
            return assignedTime;
        }
    }

    private static Serde<Windowed<String>> buildWindowedSerde() {
        return Serdes.serdeFrom(
                new WindowedSerializer<>(String().serializer()),
                new WindowedDeserializer<>(String().deserializer()));
    }

    public static class Output {
        public final String cl;
        public final String ca;

        public Output(String cl, String ca) {
            this.cl = cl;
            this.ca = ca;
        }
    }
}
