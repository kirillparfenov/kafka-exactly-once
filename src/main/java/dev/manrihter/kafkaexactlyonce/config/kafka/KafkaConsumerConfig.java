package dev.manrihter.kafkaexactlyonce.config.kafka;

import dev.manrihter.kafkaexactlyonce.model.RebalancingPartitions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    public static final String CONSUMER_1 = "CONSUMER_1";
    public static final String CONSUMER_2 = "CONSUMER_2";
    private static final String USER_GROUP = "USER_GROUP";
    @Value("${kafka.bootstrap.servers}")
    private String servers;

    private KafkaConsumer<String, String> template() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, USER_GROUP);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 5000);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 50);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaConsumer<>(config);
    }

    @Bean(CONSUMER_1)
    public KafkaConsumer<String, String> kafkaConsumer1() {
        KafkaConsumer<String, String> consumer = template();
        consumer.subscribe(List.of(KafkaTopicConfig.TRANSACTIONAL_TOPIC), new RebalancingPartitions(CONSUMER_1));
//        consumer.assign(List.of(
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 0),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 1),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 2),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 3),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 4)
//                ));
        return consumer;
    }

    @Bean(CONSUMER_2)
    public KafkaConsumer<String, String> kafkaConsumer2() {
        KafkaConsumer<String, String> consumer = template();
        consumer.subscribe(List.of(KafkaTopicConfig.TRANSACTIONAL_TOPIC), new RebalancingPartitions(CONSUMER_2));
//        consumer.assign(List.of(
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 5),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 6),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 7),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 8),
//                new TopicPartition(KafkaTopicConfig.TRANSACTIONAL_TOPIC, 9)
//        ));
        return consumer;
    }
}
