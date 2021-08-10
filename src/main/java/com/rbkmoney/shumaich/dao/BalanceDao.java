package com.rbkmoney.shumaich.dao;

import com.google.common.primitives.Longs;
import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.Balance;
import com.rbkmoney.shumaich.exception.DaoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BalanceDao extends RocksDbDao {

    private static final String COLUMN_FAMILY_NAME = "balance";

    @Override
    public byte[] getColumnFamilyName() {
        return COLUMN_FAMILY_NAME.getBytes();
    }

    public void put(Balance balance) {
        try {
            rocksDB.put(
                    columnFamilyHandle,
                    Longs.toByteArray(balance.getAccountId()),
                    CommonConverter.toBytes(balance)
            );
        } catch (RocksDBException e) {
            log.error("Can't create balance with ID: {}", balance.getAccountId(), e);
            throw new DaoException("Can't create balance with ID: " + balance.getAccountId(), e);
        }
    }

    public Balance get(Long accountId) {
        try {
            return CommonConverter.fromBytes(
                    rocksDB.get(columnFamilyHandle, Longs.toByteArray(accountId)),
                    Balance.class
            );
        } catch (RocksDBException e) {
            log.error("Can't get balance with ID: {}", accountId, e);
            throw new DaoException("Can't get balance with ID: " + accountId, e);
        }
    }

    public Balance getForUpdate(Transaction transaction, Long accountId) {
        try (ReadOptions readOptions = new ReadOptions()) {
            return CommonConverter.fromBytes(
                    transaction.get(columnFamilyHandle, readOptions, Longs.toByteArray(accountId)), Balance.class);
        } catch (RocksDBException e) {
            log.error("Can't get balance for update with ID: {}", accountId, e);
            throw new DaoException("Can't get balance for update with ID: " + accountId, e);
        }
    }

    public void putInTransaction(Transaction transaction, Balance balance) {
        try {
            transaction.put(
                    columnFamilyHandle,
                    Longs.toByteArray(balance.getAccountId()),
                    CommonConverter.toBytes(balance)
            );
        } catch (RocksDBException e) {
            log.error("Can't update balance with ID: {}", balance.getAccountId(), e);
            throw new DaoException("Can't update balance with ID: " + balance.getAccountId(), e);
        }
    }
}
