package dev.manrihter.kafkaexactlyonce.config.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.bootstrap.servers}")
    private String servers;
    public static final String TRANSACTIONAL_TOPIC = "transactional.topic";

    private static final byte PARTITIONS = 10;
    private static final byte REPLICAS = 3;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic userTopic() {
        return TopicBuilder.name(TRANSACTIONAL_TOPIC)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }
}
