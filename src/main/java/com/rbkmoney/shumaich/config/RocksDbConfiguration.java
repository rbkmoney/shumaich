package com.rbkmoney.shumaich.config;

import com.rbkmoney.shumaich.dao.RocksDbDao;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class RocksDbConfiguration {

    @Bean(destroyMethod = "close")
    TransactionDB rocksDB(@Value("${rocksdb.name}") String name,
                          @Value("${rocksdb.dir}") String dbDir,
                          List<RocksDbDao> daoList) throws RocksDBException {
        TransactionDB.loadLibrary();
        final DBOptions options = initDbOptions();
        File dbFile = new File(dbDir, name);
        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        try {
            TransactionDBOptions transactionDbOptions = new TransactionDBOptions();
            TransactionDB transactionDB = TransactionDB.open(options, transactionDbOptions, dbFile.getAbsolutePath(),
                    getColumnFamilyDescriptors(daoList), columnFamilyHandles);
            initDaos(columnFamilyHandles, daoList, transactionDB);
            return transactionDB;
        } catch (RocksDBException ex) {
            log.error("Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
                    ex.getCause(), ex.getMessage(), ex.getStackTrace());
            throw ex;
        }
    }

    private DBOptions initDbOptions() {
        final DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        return options;
    }


    private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(List<RocksDbDao> daoList) {
        List<ColumnFamilyDescriptor> descriptors = daoList.stream()
                .map(RocksDbDao::getColumnFamilyName)
                .map(ColumnFamilyDescriptor::new)
                .collect(Collectors.toList());
        descriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
        return descriptors;
    }

    private void initDaos(List<ColumnFamilyHandle> columnFamilyHandles, List<RocksDbDao> daoList, TransactionDB rocksDb) throws RocksDBException {
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            for (RocksDbDao rocksDbDao : daoList) {
                if (Arrays.equals(columnFamilyHandle.getDescriptor().getName(), rocksDbDao.getColumnFamilyName())) {
                    rocksDbDao.initDao(columnFamilyHandle, rocksDb);
                }
            }
        }
    }

}
