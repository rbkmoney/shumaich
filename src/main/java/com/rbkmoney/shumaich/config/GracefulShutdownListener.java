package com.rbkmoney.shumaich.config;

import com.rbkmoney.shumaich.dao.RocksDbDao;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class GracefulShutdownListener implements ApplicationListener<ContextClosedEvent> {

    private final TransactionDB rocksDB;
    private final List<RocksDbDao> daos;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        try {
            daos.forEach(RocksDbDao::closeHandle);
            rocksDB.syncWal();
            rocksDB.closeE();
        } catch (RocksDBException e) {
            log.error("Can't close DB properly", e);
            e.printStackTrace();
        }
    }
}