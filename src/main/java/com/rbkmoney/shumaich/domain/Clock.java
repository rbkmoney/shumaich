package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Clock {
    private String topicName;
    private List<PartitionOffsetPair> partitionOffsetPairList;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PartitionOffsetPair {

        private Integer partition;
        private Long offset;

        public PartitionOffsetPair(RecordMetadata recordMetadata) {
            this.partition = recordMetadata.partition();
            this.offset = recordMetadata.offset();
        }

        public PartitionOffsetPair(KafkaOffset kafkaOffset) {
            this.partition = kafkaOffset.getTopicPartition().partition();
            this.offset = kafkaOffset.getOffset();
        }
    }
}


