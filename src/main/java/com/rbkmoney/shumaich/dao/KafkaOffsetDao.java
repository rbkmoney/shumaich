package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.exception.DaoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaOffsetDao extends RocksDbDao {

    private static final String COLUMN_FAMILY_NAME = "kafkaOffset";

    @Override
    public byte[] getColumnFamilyName() {
        return COLUMN_FAMILY_NAME.getBytes();
    }

    public Long get(String topicPartition) {
        try {
            return CommonConverter.fromBytes(rocksDB.get(columnFamilyHandle, topicPartition.getBytes()), Long.class);
        } catch (RocksDBException e) {
            log.error("Can't get kafkaOffset topicPartition with ID: {}", topicPartition, e);
            throw new DaoException("Can't get kafkaOffset topicPartition with ID: " + topicPartition, e);
        }
    }

    public void putBatch(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
        rocksDB.write(writeOptions, writeBatch);
    }
}
