package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.shumaich.domain.Posting;
import com.rbkmoney.shumaich.domain.PostingBatch;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.utils.HashUtils;
import com.rbkmoney.shumaich.utils.WoodyTraceUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

@Component
@RequiredArgsConstructor
public class PostingPlanOperationToOperationLogListConverter {

    public List<OperationLog> convert(PostingPlanOperation source) {
        List<OperationLog> operationLogs = new ArrayList<>();
        // Each posting will be divided into 2 operations
        long planOperationsCount = 2 * source.getPostingBatches().stream()
                .mapToLong(postingBatch -> postingBatch.getPostings().size())
                .sum();
        PrimitiveIterator.OfLong sequenceId = LongStream.range(0, planOperationsCount).iterator();
        for (PostingBatch postingBatch : source.getPostingBatches()) {
            for (Posting posting : postingBatch.getPostings()) {
                long batchHash = HashUtils.computeHash(postingBatch.getPostings());
                operationLogs.add(
                        createOperationLog(source, planOperationsCount, sequenceId.next(),
                                postingBatch, posting, batchHash, true
                        )
                );
                operationLogs.add(
                        createOperationLog(source, planOperationsCount, sequenceId.next(),
                                postingBatch, posting, batchHash, false
                        )
                );
            }
        }

        addLogInfo(operationLogs);

        return operationLogs;
    }

    private void addLogInfo(List<OperationLog> operationLogs) {
        final String parentId = WoodyTraceUtils.getParentId();
        final String spanId = WoodyTraceUtils.getSpanId();
        final String traceId = WoodyTraceUtils.getTraceId();

        for (OperationLog operationLog : operationLogs) {
            operationLog.setSpanId(spanId);
            operationLog.setParentId(parentId);
            operationLog.setTraceId(traceId);
        }
    }

    private OperationLog createOperationLog(
            PostingPlanOperation source,
            long planOperationsCount,
            Long sequenceId,
            PostingBatch postingBatch,
            Posting posting,
            long batchHash,
            boolean first) {
        return new OperationLog()
                .setPlanId(source.getPlanId())
                .setBatchId(postingBatch.getId())
                .setOperationType(source.getOperationType())
                .setAccount(first
                        ? posting.getToAccount()
                        : posting.getFromAccount())
                .setAmountWithSign(first
                        ? posting.getAmount()
                        : Math.negateExact(posting.getAmount()))
                .setCurrencySymbolicCode(posting.getCurrencySymbolicCode())
                .setDescription(posting.getDescription())
                .setSequenceId(sequenceId)
                .setPlanOperationsCount(planOperationsCount)
                .setBatchHash(batchHash)
                .setValidationError(source.getValidationError())
                .setCreationTimeMs(source.getCreationTime().atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
    }
}
