package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumaich.Clock;
import com.rbkmoney.damsel.shumaich.LatestClock;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.exception.NotReadyException;
import com.rbkmoney.shumaich.helpers.TestUtils;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClockServiceTest {

    KafkaOffsetService kafkaOffsetService = mock(KafkaOffsetService.class);
    ClockService service = new ClockService(kafkaOffsetService);

    @Test
    public void successPath() {
        List<RecordMetadata> recordMetadata = List.of(
                getRecordMetadata(1, 10),
                getRecordMetadata(2, 7),
                getRecordMetadata(3, 25)
        );

        List<KafkaOffset> kafkaOffsets = service.parseClock(service.formClock(recordMetadata));
        kafkaOffsets.sort(Comparator.comparing(kafkaOffset -> kafkaOffset.getTopicPartition().partition()));

        assertOffsetsEquals(recordMetadata, kafkaOffsets);
    }

    @Test
    public void emptyClock() {
        Assert.assertTrue(service.parseClock("").isEmpty());
    }

    @Test
    public void checkClockTimeline_success() {
        when(kafkaOffsetService.isBeforeCurrentOffsets(any())).thenReturn(true);
        service.hardCheckClockTimeline(getFilledClock());
        service.softCheckClockTimeline(getFilledClock());
        service.softCheckClockTimeline(getEmptyClock());
        service.softCheckClockTimeline(getLatestClock());
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkClockTimeline_latest() {
        service.hardCheckClockTimeline(getLatestClock());
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkClockTimeline_empty() {
        service.hardCheckClockTimeline(getEmptyClock());
    }

    @Test(expected = NotReadyException.class)
    public void checkClockTimeline_notReady() {
        when(kafkaOffsetService.isBeforeCurrentOffsets(any())).thenReturn(false);
        service.hardCheckClockTimeline(getFilledClock());
    }

    private RecordMetadata getRecordMetadata(Integer partition, Integer offset) {
        return new RecordMetadata(
                new TopicPartition("test", partition),
                0,
                offset,
                0,
                0L,
                0,
                0
        );
    }

    private void assertOffsetsEquals(List<RecordMetadata> recordMetadataList, List<KafkaOffset> kafkaOffsets) {
        for (int i = 0; i < kafkaOffsets.size(); i++) {
            KafkaOffset kafkaOffset = kafkaOffsets.get(i);
            RecordMetadata recordMetadata = recordMetadataList.get(i);
            assertEquals(kafkaOffset.getOffset().longValue(), recordMetadata.offset());
            assertEquals(kafkaOffset.getTopicPartition().topic(), recordMetadata.topic());
            assertEquals(kafkaOffset.getTopicPartition().partition(), recordMetadata.partition());
        }
    }

    private Clock getLatestClock() {
        return Clock.latest(new LatestClock());
    }

    private Clock getFilledClock() {
        return Clock.vector(VectorClockSerde.serialize(TestUtils.createSerializedClock()));
    }

    private Clock getEmptyClock() {
        return Clock.vector(VectorClockSerde.serialize(""));
    }
}
