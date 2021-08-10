package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.Account;
import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class PostingPlanOperationToOperationLogListConverterTest {

    PostingPlanOperationToOperationLogListConverter converter = new PostingPlanOperationToOperationLogListConverter();

    @Test
    public void noGapsInSequence() {
        PostingPlanOperation postingPlanOperation = TestData.postingPlanOperation();
        List<OperationLog> operationLogs = converter.convert(postingPlanOperation);

        assertEquals(operationLogs.get(0).getPlanOperationsCount(), operationLogs.size());

        List<OperationLog> orderedOperationLogs = operationLogs.stream()
                .sorted(Comparator.comparingLong(OperationLog::getSequenceId))
                .collect(Collectors.toList());

        for (int i = 0; i < orderedOperationLogs.size(); i++) {
            OperationLog operationLog = orderedOperationLogs.get(i);
            assertEquals(i, (int) operationLog.getSequenceId());
            assertEquals(orderedOperationLogs.size(), (int) operationLog.getPlanOperationsCount());
        }
    }

    @Test
    public void equalMoneyDistribution() {
        PostingPlanOperation postingPlanOperation = TestData.postingPlanOperation();
        List<OperationLog> operationLogs = converter.convert(postingPlanOperation);

        Map<Account, Long> postingPlanMoneyMap = new HashMap<>();

        postingPlanOperation.getPostingBatches().stream()
                .flatMap(postingBatch -> postingBatch.getPostings().stream())
                .forEach(posting -> {
                    if (postingPlanMoneyMap.containsKey(posting.getToAccount())) {
                        postingPlanMoneyMap.put(
                                posting.getToAccount(),
                                postingPlanMoneyMap.get(posting.getToAccount()) + posting.getAmount()
                        );
                    }
                    if (postingPlanMoneyMap.containsKey(posting.getFromAccount())) {
                        postingPlanMoneyMap.put(
                                posting.getFromAccount(),
                                postingPlanMoneyMap.get(posting.getFromAccount()) +
                                Math.negateExact(posting.getAmount())
                        );
                    }
                    postingPlanMoneyMap.putIfAbsent(posting.getToAccount(), posting.getAmount());
                    postingPlanMoneyMap.putIfAbsent(posting.getFromAccount(), Math.negateExact(posting.getAmount()));
                });

        Map<Account, Long> operationLogMoneyMap = operationLogs.stream()
                .collect(Collectors.groupingBy(OperationLog::getAccount))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        operationLogEntry -> operationLogEntry.getValue().stream()
                                .mapToLong(OperationLog::getAmountWithSign)
                                .reduce(0, Long::sum)
                ));

        assertEquals(postingPlanMoneyMap, operationLogMoneyMap);
    }
}
