package com.rbkmoney.shumaich.dao;


import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.Plan;
import com.rbkmoney.shumaich.exception.DaoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Slf4j
@Component
@RequiredArgsConstructor
public class PlanDao extends RocksDbDao {

    private static final String COLUMN_FAMILY_NAME = "plan";

    @Override
    public byte[] getColumnFamilyName() {
        return COLUMN_FAMILY_NAME.getBytes();
    }

    public Plan get(String planId) {
        try {
            return CommonConverter.fromBytes(rocksDB.get(columnFamilyHandle, planId.getBytes()), Plan.class);
        } catch (RocksDBException e) {
            log.error("Can't get plan with id: {}", planId, e);
            throw new DaoException("Can't get plan with id: " + planId, e);
        }
    }

    public Plan getForUpdate(Transaction transaction, String planId) {
        try (ReadOptions readOptions = new ReadOptions()) {
            return CommonConverter.fromBytes(
                    transaction.getForUpdate(readOptions, columnFamilyHandle, planId.getBytes(), true),
                    Plan.class
            );
        } catch (RocksDBException e) {
            log.error("Can't get plan for update with id: {}", planId, e);
            throw new DaoException("Can't get plan for update with id: " + planId, e);
        }
    }

    public void putInTransaction(Transaction transaction, String planId, Plan plan) {
        try {
            transaction.put(columnFamilyHandle, planId.getBytes(), CommonConverter.toBytes(plan));
        } catch (RocksDBException e) {
            log.error("Can't save plan with id: {}", planId, e);
            throw new DaoException("Can't save plan with id: " + planId, e);
        }
    }

    public void delete(String planId) {
        try {
            rocksDB.delete(columnFamilyHandle, planId.getBytes());
        } catch (RocksDBException e) {
            log.error("Can't delete plan with id: {}", planId, e);
            throw new DaoException("Can't delete plan with id: " + planId, e);
        }
    }

    @PreDestroy
    @Override
    public void closeHandle() {
        super.closeHandle();
    }
}
