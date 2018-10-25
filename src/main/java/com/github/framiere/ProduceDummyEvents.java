package com.github.framiere;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ProduceDummyEvents {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String[] ACTIONS = new String[]{"CONNECTION", "DISCONNECTION", "UP", "DOWN", "ACTION1", "ACTION2", "ACTION3", "ACTION4", "ACTION5", "ACTION6"};
    private static final String[] LANGUAGES = new String[]{"EN", "FR", "ESP", "RUS", "NONE"};
    private static final String[] AUDIO_MODE = new String[]{"STEREO", "MONO"};
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.getDefault());

    public static void main(String[] args) throws Exception {
        try (Producer<String, String> producer = new KafkaProducer<>(buildProperties())) {
            for (int i = 1; i < 100_000_000; i++) {
                Event value = buildRandomInputModel();
                producer.send(new ProducerRecord<>("input", value.deviceID, OBJECT_MAPPER.writeValueAsString(value)));
                if (i % 200_000 == 0) {
                    System.out.println("Sent " + NUMBER_FORMAT.format(i) + " events");
                }
            }
        }
    }

    private static Properties buildProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 4 * 1024 * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, TimeUnit.SECONDS.toMillis(1));
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    private static Event buildRandomInputModel() {
        Event value = new Event();
        int deviceId = RandomUtils.nextInt(100_000);

        value.eventID = UUID.randomUUID().toString();
        value.timeStamp = System.currentTimeMillis();
        value.adminCode = "06311233286988" + deviceId;
        value.deviceID = "1.1.1.1.1_F0FCC8F7" + deviceId;
        value.action = ACTIONS[RandomUtils.nextInt(ACTIONS.length)];
        value.channelUID = "" + RandomUtils.nextInt(200);
        value.audioLang = LANGUAGES[RandomUtils.nextInt(LANGUAGES.length)];
        value.audioMode = AUDIO_MODE[RandomUtils.nextInt(AUDIO_MODE.length)];
        value.subtitleLang = LANGUAGES[RandomUtils.nextInt(LANGUAGES.length)];
        value.dwMode = "STB_IPTV";
        value.dman = "arris";
        value.dmod = "STIH207-0.0";
        value.originalTS = value.timeStamp;
        value.sourceIP = "NONE";
        value.connectionType = 1;
        value.sessionUserProfile = 0;
        return value;
    }
}
