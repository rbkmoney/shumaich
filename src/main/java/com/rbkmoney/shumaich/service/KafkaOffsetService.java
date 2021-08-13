package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.exception.DaoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaOffsetService {

    private final KafkaOffsetDao kafkaOffsetDao;

    public List<KafkaOffset> loadOffsets(Collection<TopicPartition> topicPartitions) {
        return topicPartitions.stream()
                .map(this::getKafkaOffset)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private KafkaOffset getKafkaOffset(TopicPartition topicPartition) {
        Long offset = kafkaOffsetDao.get(topicPartition.toString());

        return offset == null ? null : new KafkaOffset(topicPartition, offset);
    }

    public void saveOffsets(List<KafkaOffset> kafkaOffsets) {
        try (WriteBatch writeBatch = new WriteBatch();
                WriteOptions writeOptions = new WriteOptions().setSync(true)) {
            prepareBatch(kafkaOffsets, writeBatch);
            kafkaOffsetDao.putBatch(writeOptions, writeBatch);
        } catch (Throwable e) {
            log.error("Putting kafkaOffset to writeBatch exception:{}", kafkaOffsets, e);
            throw new DaoException("Putting kafkaOffset to writeBatch exception: " + kafkaOffsets, e);
        }
    }

    private void prepareBatch(List<KafkaOffset> kafkaOffsets, WriteBatch writeBatch) throws RocksDBException {
        for (Map.Entry<String, Long> entry : convertToMap(kafkaOffsets).entrySet()) {
            writeBatch.put(kafkaOffsetDao.getColumnFamilyHandle(), entry.getKey().getBytes(),
                    CommonConverter.toBytes(entry.getValue())
            );
        }
    }

    public boolean isBeforeCurrentOffsets(List<KafkaOffset> clockKafkaOffsets) {
        List<TopicPartition> clockPartitions = clockKafkaOffsets.stream()
                .map(KafkaOffset::getTopicPartition)
                .collect(toList());

        List<KafkaOffset> currentOffsets = loadOffsets(clockPartitions);

        Map<Integer, Long> currentOffsetsLookupMap = currentOffsets.stream()
                .collect(Collectors.toMap(ko -> ko.getTopicPartition().partition(), KafkaOffset::getOffset));

        return clockKafkaOffsets.stream().allMatch(isBefore(currentOffsetsLookupMap));
    }

    private Predicate<KafkaOffset> isBefore(Map<Integer, Long> currentOffsetsLookupMap) {
        return clockKafkaOffset -> {
            Long currentOffset = currentOffsetsLookupMap.get(clockKafkaOffset.getTopicPartition().partition());
            if (currentOffset == null) {
                return false;
            }
            return currentOffset > clockKafkaOffset.getOffset();
        };
    }

    private Map<String, Long> convertToMap(List<KafkaOffset> kafkaOffsets) {
        return kafkaOffsets.stream().collect(
                Collectors.toMap(
                        kafkaOffset -> kafkaOffset.getTopicPartition().toString(),
                        KafkaOffset::getOffset
                )
        );
    }

}
