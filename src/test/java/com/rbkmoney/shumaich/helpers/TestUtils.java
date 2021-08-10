package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumaich.Clock;
import com.rbkmoney.damsel.shumaich.PostingPlan;
import com.rbkmoney.damsel.shumaich.PostingPlanChange;
import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.service.ClockService;
import com.rbkmoney.shumaich.utils.VectorClockSerde;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtils {

    private static final ClockService CLOCK_SERVICE = new ClockService(null);

    public static Clock moveClockFurther(Clock clock, Map<Long, Long> partitionsAndIncrement) {
        List<KafkaOffset> kafkaOffsets = CLOCK_SERVICE.parseClock(VectorClockSerde.deserialize(clock.getVector()));
        kafkaOffsets.forEach(kafkaOffset -> {
            int partition = kafkaOffset.getTopicPartition().partition();
            if (partitionsAndIncrement.containsKey((long) partition)) {
                kafkaOffset.setOffset(kafkaOffset.getOffset() + partitionsAndIncrement.get((long) partition));
            }
        });
        return Clock.vector(
                VectorClockSerde.serialize(
                        CommonConverter.serialize(
                                new com.rbkmoney.shumaich.domain.Clock(
                                        kafkaOffsets.get(0).getTopicPartition().topic(),
                                        kafkaOffsets
                                                .stream()
                                                .map(com.rbkmoney.shumaich.domain.Clock.PartitionOffsetPair::new)
                                                .collect(Collectors.toList())
                                ))
                )
        );
    }

    public static String createSerializedClock() {
        return CommonConverter.serialize(
                new com.rbkmoney.shumaich.domain.Clock(
                        "test",
                        List.of(
                                new com.rbkmoney.shumaich.domain.Clock.PartitionOffsetPair(1, 1L),
                                new com.rbkmoney.shumaich.domain.Clock.PartitionOffsetPair(2, 2L)
                        )
                )
        );
    }

    public static PostingPlan postingPlanFromPostingPlanChange(PostingPlanChange postingPlanChange) {
        return new PostingPlan(postingPlanChange.getId(), List.of(postingPlanChange.getBatch()));
    }

}
