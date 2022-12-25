package dev.manrihter.kafkaexactlyonce.model;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalancingPartitions implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(RebalancingPartitions.class);

    private final String consumerName;

    public RebalancingPartitions(String consumerName) {
        this.consumerName = consumerName;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("У консьюмера [{}] отозваны партиции: {}", consumerName, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Консьюмеру [{}] назначены партиции: {}", consumerName, partitions);
    }
}
