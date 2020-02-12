package com.rbkmoney.shumaich.proofofconcept;

import com.rbkmoney.shumaich.proofofconcept.domain.Balance;
import com.rbkmoney.shumaich.proofofconcept.domain.Plan;
import com.rbkmoney.shumaich.proofofconcept.domain.exception.NotReadyException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.rbkmoney.shumaich.domain.OperationType.*;

@Slf4j
@RequiredArgsConstructor
public class Client implements Runnable {

    private final Server server;
    private final Integer accFrom;
    private final Integer accTo;
    private final Integer amount;
    private final String planId;

    @Override
    public void run() {
        Plan hold = Utils.createPlanWithPostings(planId, HOLD, Utils.createPosting(accFrom, accTo, amount));
        Plan commit = Utils.createPlanWithPostings(planId, COMMIT, Utils.createPosting(accFrom, accTo, amount));
        Plan rollback = Utils.createPlanWithPostings(planId, ROLLBACK, Utils.createPosting(accFrom, accTo, amount));

        List<Integer> clock = server.makeOperation(hold);

        server.calculateBalancesToLatest();

        Balance balance;

        while (true) {
            try {
                balance = server.getBalance(accFrom, clock);
                break;
            } catch (NotReadyException ex) {}
        }

        if (balance.getMinAmount().get() >= 0) {
            server.makeOperation(commit);
        } else {
            server.makeOperation(rollback);
        }

    }

}
