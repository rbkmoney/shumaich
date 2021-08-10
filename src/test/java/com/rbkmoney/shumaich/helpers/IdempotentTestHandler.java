package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.shumaich.kafka.handler.Handler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class IdempotentTestHandler implements Handler<Long, OperationLog> {

    Map<String, Set<Long>> receivedRecords = new HashMap<>();

    @Override
    public void handle(ConsumerRecords<Long, OperationLog> records) {
        for (ConsumerRecord<?, OperationLog> record : records) {
            if (record.value() == null) {
                continue;
            }
            OperationLog value = record.value();
            receivedRecords.putIfAbsent(value.getPlanId(), new HashSet<>());
            receivedRecords.get(value.getPlanId()).add(value.getSequenceId());
        }
    }

    public Integer countReceivedRecords(String planId) {
        return receivedRecords.get(planId).size();
    }
}
