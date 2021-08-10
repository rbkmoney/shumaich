package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.Clock;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.exception.NotReadyException;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClockService {

    private final KafkaOffsetService kafkaOffsetService;

    public String formClock(List<RecordMetadata> recordMetadataList) {
        Clock clock = new Clock(
                recordMetadataList.get(0).topic(),
                recordMetadataList.stream()
                        .map(Clock.PartitionOffsetPair::new)
                        .collect(Collectors.toList())
        );
        return CommonConverter.serialize(clock);
    }

    public List<KafkaOffset> parseClock(String clock) {
        if (isBlank(clock)) {
            return Collections.emptyList();
        }
        Clock deserializedClock = CommonConverter.deserialize(clock, Clock.class);

        return deserializedClock.getPartitionOffsetPairList()
                .stream()
                .map(partitionOffsetPair -> new KafkaOffset(
                        new TopicPartition(deserializedClock.getTopicName(), partitionOffsetPair.getPartition()),
                        partitionOffsetPair.getOffset()
                ))
                .collect(Collectors.toList());
    }

    public void hardCheckClockTimeline(com.rbkmoney.damsel.shumaich.Clock clock) {
        if (clock == null || clock.isSetLatest()) {
            throw new IllegalArgumentException("Clock can't be latest");
        }

        List<KafkaOffset> kafkaOffsets = parseClock(VectorClockSerde.deserialize(clock.getVector()));

        if (kafkaOffsets.isEmpty()) {
            throw new IllegalArgumentException("Clock can't be empty");
        }

        if (!kafkaOffsetService.isBeforeCurrentOffsets(kafkaOffsets)) {
            throw new NotReadyException();
        }

    }

    public void softCheckClockTimeline(com.rbkmoney.damsel.shumaich.Clock clock) {
        if (clock == null || clock.isSetLatest()) {
            return;
        }

        List<KafkaOffset> kafkaOffsets = parseClock(VectorClockSerde.deserialize(clock.getVector()));

        if (kafkaOffsets.isEmpty()) {
            return;
        }

        if (!kafkaOffsetService.isBeforeCurrentOffsets(kafkaOffsets)) {
            throw new NotReadyException();
        }

    }
}
