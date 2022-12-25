package dev.manrihter.kafkaexactlyonce.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Mapper
public interface KafkaExactlyOnceMapper {
    void saveTopicOffset(@Param("records") List<ConsumerRecord<String, String>> records);
}
