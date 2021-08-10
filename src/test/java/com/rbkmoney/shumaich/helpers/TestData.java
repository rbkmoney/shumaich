package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumaich.*;
import com.rbkmoney.shumaich.converter.PostingBatchDamselToPostingBatchConverter;
import com.rbkmoney.shumaich.converter.PostingDamselToPostingConverter;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import org.apache.kafka.common.TopicPartition;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class TestData {

    public static final String TEST_TOPIC = "test_topic";
    public static final String OPERATION_LOG_TOPIC = "operation_log";
    public static final Long PROVIDER_ACC = 1000000000L;
    public static final Long SYSTEM_ACC = 2000000000L;
    public static final Long MERCHANT_ACC = 3000000000L;
    public static final String PLAN_ID = "plan";

    private static final PostingBatchDamselToPostingBatchConverter CONVERTER =
            new PostingBatchDamselToPostingBatchConverter(
                    new PostingDamselToPostingConverter()
            );

    public static PostingPlanOperation postingPlanOperation(String planId) {
        return PostingPlanOperation.builder()
                .planId(planId)
                .operationType(OperationType.HOLD)
                .creationTime(LocalDateTime.now())
                .postingBatches(
                        List.of(
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC),
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC)
                        ).stream().map(CONVERTER::convert).collect(Collectors.toList())
                )
                .build();
    }

    public static PostingPlanOperation postingPlanOperation() {
        return PostingPlanOperation.builder()
                .planId("plan")
                .operationType(OperationType.HOLD)
                .creationTime(LocalDateTime.now())
                .postingBatches(
                        List.of(
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC),
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC)
                        ).stream().map(CONVERTER::convert).collect(Collectors.toList())
                )
                .build();
    }

    public static KafkaOffset kafkaOffset(String topicName, Integer partition, Long offset) {
        return KafkaOffset.builder()
                .topicPartition(new TopicPartition(topicName, partition))
                .offset(offset)
                .build();
    }

    public static TopicPartition topicPartition(int partition) {
        return new TopicPartition(TEST_TOPIC, partition);
    }

    public static PostingPlanChange postingPlanChange() {
        return PostingGenerator.createPostingPlanChange(PLAN_ID, PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }

    public static PostingPlanChange postingPlanChange(String planId) {
        return PostingGenerator.createPostingPlanChange(planId, PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }

    public static PostingPlan postingPlan() {
        return PostingGenerator.createPostingPlan(PLAN_ID, PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }

    public static PostingPlan postingPlan(String planId) {
        return PostingGenerator.createPostingPlan(planId, PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }

    public static Posting postingDamsel() {
        return PostingGenerator.createPosting();
    }

    public static PostingBatch postingBatchDamsel() {
        return PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }
}
