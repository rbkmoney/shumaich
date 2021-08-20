package com.rbkmoney.shumaich.dao;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.TransactionDB;

public abstract class RocksDbDao {

    protected ColumnFamilyHandle columnFamilyHandle;

    protected TransactionDB rocksDB;

    public abstract byte[] getColumnFamilyName();

    public void initDao(ColumnFamilyHandle columnFamilyHandle, TransactionDB rocksDB) {
        this.columnFamilyHandle = columnFamilyHandle;
        this.rocksDB = rocksDB;
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

    public void closeHandle() {
        this.columnFamilyHandle.close();
    }
}
