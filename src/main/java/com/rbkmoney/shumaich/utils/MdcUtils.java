package com.rbkmoney.shumaich.utils;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.damsel.shumaich.PostingPlan;
import com.rbkmoney.damsel.shumaich.PostingPlanChange;
import com.rbkmoney.woody.api.MDCUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.slf4j.MDC;

import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MdcUtils {
    public static final String PLAN_ID = "plan_id";
    public static final String ACCOUNT_ID = "account_id";

    public static void setMdc(OperationLog operationLog) {
        MDC.put(PLAN_ID, operationLog.getPlanId());
        MDC.put(ACCOUNT_ID, Long.toString(operationLog.getAccount().getId()));
        if (WoodyTraceUtils.getActiveSpan() != null) {
            MDCUtils.putSpanData(WoodyTraceUtils.getActiveSpan());
        }
    }

    public static void setMdc(PostingPlanChange postingPlanChange) {
        MDC.put(PLAN_ID, postingPlanChange.getId());
        MDC.put(ACCOUNT_ID, extractAccounts(postingPlanChange));
    }

    public static void setMdc(PostingPlan postingPlan) {
        MDC.put(PLAN_ID, postingPlan.getId());
        MDC.put(ACCOUNT_ID, extractAccounts(postingPlan));
    }

    public static void setMdc(Long accountId) {
        MDC.put(ACCOUNT_ID, accountId.toString());
    }

    public static void clearMdc() {
        MDC.clear();
    }

    private static String extractAccounts(PostingPlanChange postingPlanChange) {
        return postingPlanChange.getBatch().getPostings()
                .stream()
                .flatMap(posting -> Stream.of(posting.getFromAccount().getId(), posting.getToAccount().getId()))
                .distinct()
                .map(Object::toString)
                .collect(Collectors.joining(","));
    }

    private static String extractAccounts(PostingPlan postingPlan) {
        return postingPlan.getBatchList().stream()
                .flatMap(batch -> batch.getPostings().stream())
                .flatMap(posting -> Stream.of(posting.getFromAccount().getId(), posting.getToAccount().getId()))
                .distinct()
                .map(Object::toString)
                .collect(Collectors.joining(","));
    }
}
