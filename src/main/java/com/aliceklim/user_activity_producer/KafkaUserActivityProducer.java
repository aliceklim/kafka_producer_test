package com.aliceklim.user_activity_producer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaUserActivityProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostConstruct
    public void produceUserActivities() throws InterruptedException {
        String user_topic = "user-activity";
        String admin_topic = "admin-activity";

        for (int i = 1; i <= 10; i++) {
            if (i % 2 == 0){
                String adminActivity = String.format(
                        "{\"user_id\": %d, \"action\": \"click\", \"timestamp\": %d}",
                        i, System.currentTimeMillis()
                );
                kafkaTemplate.send(admin_topic, adminActivity);
            }
            String userActivity = String.format(
                    "{\"user_id\": %d, \"action\": \"click\", \"timestamp\": %d}",
                    i, System.currentTimeMillis()
            );
            Thread.sleep(getTimeout());
            kafkaTemplate.send(user_topic, String.valueOf(i), userActivity);
        }
    }

    private long getTimeout(){
        int timeout = new Random().nextInt(5) + 1;
        return timeout * 1000L;
    }
}
