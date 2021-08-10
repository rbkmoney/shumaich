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
public class HoldPlansExecutor implements Callable<Map.Entry<String, Balance>> {

    private final ShumaichServiceHandler serviceHandler;
    private final PostingPlanChange postingPlanChange;
    private final RetryTemplate retryTemplate;
    private final Long accountToCheck;

    @Override
    public Map.Entry<String, Balance> call() throws Exception {
        Clock holdClock = retryTemplate.execute(context -> serviceHandler.hold(postingPlanChange, null));
        Balance balanceByID =
                retryTemplate.execute(context -> serviceHandler.getBalanceByID(accountToCheck, holdClock));
        return Map.entry(VectorClockSerde.deserialize(balanceByID.getClock().getVector()), balanceByID);
    }
}