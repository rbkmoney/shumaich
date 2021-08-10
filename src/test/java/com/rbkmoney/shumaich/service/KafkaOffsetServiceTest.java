package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.RocksdbTestBase;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.helpers.TestData;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

@Slf4j
@ContextConfiguration(classes = {KafkaOffsetDao.class, KafkaOffsetService.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class KafkaOffsetServiceTest extends RocksdbTestBase {

    @Autowired
    KafkaOffsetService kafkaOffsetService;

    @Test
    public void saveAndLoad() {
        kafkaOffsetService.saveOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L)
        ));

        List<KafkaOffset> kafkaOffsets = kafkaOffsetService.loadOffsets(List.of(
                TestData.topicPartition(1),
                TestData.topicPartition(2),
                TestData.topicPartition(3)
        ));

        kafkaOffsets.forEach(offset -> Assert.assertEquals(1L, offset.getOffset().longValue()));
    }

    @Test
    public void loadNotExistedOffsets() {
        List<KafkaOffset> kafkaOffsets = kafkaOffsetService.loadOffsets(List.of(
                TestData.topicPartition(1),
                TestData.topicPartition(2),
                TestData.topicPartition(3)
        ));

        Assert.assertEquals(0, kafkaOffsets.size());
    }

    @Test
    public void rewriteExistingOffsets() {
        kafkaOffsetService.saveOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L)
        ));

        kafkaOffsetService.saveOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 10L)
        ));

        List<KafkaOffset> kafkaOffsets = kafkaOffsetService.loadOffsets(List.of(
                TestData.topicPartition(1),
                TestData.topicPartition(2),
                TestData.topicPartition(3)
        ));

        kafkaOffsets.forEach(offset -> Assert.assertEquals(10L, offset.getOffset().longValue()));

    }

    @Test
    public void checkOffsetsOrder() {
        kafkaOffsetService.saveOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 10L)
        ));

        Assert.assertFalse(kafkaOffsetService.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 10L)
        )));

        Assert.assertTrue(kafkaOffsetService.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 9L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 6L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L)
        )));

        Assert.assertTrue(kafkaOffsetService.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 6L)
        )));

        Assert.assertTrue(kafkaOffsetService.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 6L)
        )));

        Assert.assertFalse(kafkaOffsetService.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L)
        )));

        Assert.assertFalse(kafkaOffsetService.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 15L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 15L)
        )));

        Assert.assertFalse(kafkaOffsetService.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 15L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 15L)
        )));

    }

}
