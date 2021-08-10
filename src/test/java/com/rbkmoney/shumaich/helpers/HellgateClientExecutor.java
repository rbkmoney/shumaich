package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumaich.Balance;
import com.rbkmoney.damsel.shumaich.Clock;
import com.rbkmoney.damsel.shumaich.PostingPlanChange;
import com.rbkmoney.shumaich.handler.ShumaichServiceHandler;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import lombok.RequiredArgsConstructor;
import org.springframework.retry.support.RetryTemplate;

import java.util.Map;
import java.util.concurrent.Callable;

@RequiredArgsConstructor
public class HellgateClientExecutor implements Callable<Map.Entry<String, Balance>> {

    private final ShumaichServiceHandler serviceHandler;
    private final PostingPlanChange postingPlanChange;
    private final RetryTemplate retryTemplate;
    private final Long account;

    @Override
    public Map.Entry<String, Balance> call() throws Exception {
        Clock holdClock = retryTemplate.execute(context -> serviceHandler.hold(postingPlanChange, null));
        Balance holdBalance = retryTemplate.execute(context -> serviceHandler.getBalanceByID(account, holdClock));
        Clock clockAfterFinalOperation;
        if (holdBalance.getMinAvailableAmount() >= 0) {
            clockAfterFinalOperation = retryTemplate.execute(context -> serviceHandler.commitPlan(
                    TestUtils.postingPlanFromPostingPlanChange(postingPlanChange), holdClock));
        } else {
            clockAfterFinalOperation = retryTemplate.execute(context -> serviceHandler.rollbackPlan(
                    TestUtils.postingPlanFromPostingPlanChange(postingPlanChange), holdClock));
        }
        Balance finalOpBalance =
                retryTemplate.execute(context -> serviceHandler.getBalanceByID(account, clockAfterFinalOperation));
        return Map.entry(VectorClockSerde.deserialize(finalOpBalance.getClock().getVector()), finalOpBalance);
    }
}