package com.aliceklim.user_activity_producer;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.*;

@Slf4j
@Component
public class KafkaUserActivityProducer {
    @PostConstruct
    public void produceUserActivities() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String topic = "user-activity";

            for (int i = 1; i <= 10; i++) {
                String userActivity = String.format(
                        "{\"user_id\": %d, \"action\": \"click\", \"timestamp\": %d}",
                        i, System.currentTimeMillis()
                );
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, Integer.toString(i), userActivity);
                producer.send(record);
            }

            log.info("User activities sent successfully.");
        }
    }
}
