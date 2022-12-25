package dev.manrihter.kafkaexactlyonce.config.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap.servers}")
    private String servers;

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 256);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 1);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // устранение дублей
//        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_0"); // для транзакционности должно быть не менее 3 брокеров!
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configProps));
    }
}
