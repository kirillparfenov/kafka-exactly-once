package dev.manrihter.kafkaexactlyonce.service;

import dev.manrihter.kafkaexactlyonce.config.kafka.KafkaConsumerConfig;
import dev.manrihter.kafkaexactlyonce.config.kafka.KafkaTopicConfig;
import dev.manrihter.kafkaexactlyonce.dao.KafkaExactlyOnceMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class ExactlyOnceService {
    private static final Logger log = LoggerFactory.getLogger(ExactlyOnceService.class);
    private static final Integer MESSAGES_SIZE = 500_000;
    private static final Integer BUFFER_SIZE = 30_000;

    private final KafkaExactlyOnceMapper exactlyOnceMapper;
    private final KafkaTemplate<String, String> kafkaProducer;
    private final KafkaConsumer<String, String> kafkaConsumer1;
    private final KafkaConsumer<String, String> kafkaConsumer2;

    private final ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(5);

    @Autowired
    public ExactlyOnceService(
            final KafkaExactlyOnceMapper exactlyOnceMapper,
            final KafkaTemplate<String, String> kafkaProducer,
            @Qualifier(KafkaConsumerConfig.CONSUMER_1) final KafkaConsumer<String, String> kafkaConsumer1,
            @Qualifier(KafkaConsumerConfig.CONSUMER_2) final KafkaConsumer<String, String> kafkaConsumer2
    ) {
        this.exactlyOnceMapper = exactlyOnceMapper;
        this.kafkaProducer = kafkaProducer;
        this.kafkaConsumer1 = kafkaConsumer1;
        this.kafkaConsumer2 = kafkaConsumer2;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        scheduled.execute(this::produce);
        scheduled.schedule(this::consume1, 2, TimeUnit.SECONDS);
        scheduled.schedule(this::consume2, 2, TimeUnit.SECONDS);
    }

    void produce() {
        StopWatch watch = new StopWatch();
        watch.start();

        for (int i = 0; i < MESSAGES_SIZE; i++) {
            kafkaProducer.send(KafkaTopicConfig.TRANSACTIONAL_TOPIC, String.valueOf(i), "Сообщение: " + i);
        }

        watch.stop();
        log.info("Времени затрачено на отправку {} записей: {}ms", MESSAGES_SIZE, watch.getLastTaskTimeMillis());
    }

    // https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
    void consume1() {
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (true) {
                var records = kafkaConsumer1.poll(Duration.ofMillis(1000));

                for (TopicPartition partition : records.partitions()) {
                    buffer.addAll(records.records(partition));

                    var partitionRecords = records.records(partition);
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    kafkaConsumer1.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }

                if (buffer.size() >= BUFFER_SIZE || (buffer.size() > 0 && records.count() == 0)) {
//                    exactlyOnceMapper.saveTopicOffset(buffer);
                    log.info("Consumer1, buffer saved: {}", buffer.size());
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            log.error("Ошибка во время обработки сообщения из Кафки: {}", e.getMessage());
        } finally {
            kafkaConsumer1.close();
        }
    }

    void consume2() {
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (true) {
                var records = kafkaConsumer2.poll(Duration.ofMillis(1000));

                for (TopicPartition partition : records.partitions()) {
                    buffer.addAll(records.records(partition));

                    var partitionRecords = records.records(partition);
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    kafkaConsumer2.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }

                if (buffer.size() >= BUFFER_SIZE || (buffer.size() > 0 && records.count() == 0)) {
//                    exactlyOnceMapper.saveTopicOffset(buffer);
                    log.info("Consumer2, buffer saved: {}", buffer.size());
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            log.error("Ошибка во время обработки сообщения из Кафки: {}", e.getMessage());
        } finally {
            kafkaConsumer2.close();
        }
    }
}