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

    @Bean(destroyMethod = "closeE")
    TransactionDB rocksDB(
            @Value("${rocksdb.name}") String name,
            @Value("${rocksdb.dir}") String dbDir,
            List<RocksDbDao> daoList,
            DBOptions dbOptions,
            TransactionDBOptions transactionDbOptions) throws RocksDBException {
        try {
            File dbFile = new File(dbDir, name);
            ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            TransactionDB transactionDB = TransactionDB.open(dbOptions, transactionDbOptions, dbFile.getAbsolutePath(),
                    getColumnFamilyDescriptors(daoList), columnFamilyHandles
            );
            Thread.sleep(5000L); //todo fix this delay
            initDaos(columnFamilyHandles, daoList, transactionDB);
            Thread.sleep(5000L); //todo fix this delay
            return transactionDB;
        } catch (RocksDBException ex) {
            log.error("Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, " +
                      "stackTrace: {}",
                    ex.getCause(),
                    ex.getMessage(),
                    ex.getStackTrace()
            );
            throw ex;
        } catch (InterruptedException e) {
            log.error("OMGOD", e);
            throw new RuntimeException();
        }
    }

    @Bean(destroyMethod = "close")
    public DBOptions dbOptions() {
        final DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
        return options;
    }

    @Bean(destroyMethod = "close")
    public TransactionDBOptions transactionDbOptions() {
        return new TransactionDBOptions();
    }

    private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(List<RocksDbDao> daoList) {
        List<ColumnFamilyDescriptor> descriptors = daoList.stream()
                .map(RocksDbDao::getColumnFamilyName)
                .map(ColumnFamilyDescriptor::new)
                .collect(Collectors.toList());
        descriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
        return descriptors;
    }

    private void initDaos(List<ColumnFamilyHandle> columnFamilyHandles, List<RocksDbDao> daoList, TransactionDB rocksDb)
            throws RocksDBException {
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            for (RocksDbDao rocksDbDao : daoList) {
                if (Arrays.equals(columnFamilyHandle.getDescriptor().getName(), rocksDbDao.getColumnFamilyName())) {
                    rocksDbDao.initDao(columnFamilyHandle, rocksDb);
                }
            }
        }
    }

}
