package com.rbkmoney.shumaich.proofofconcept;

import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.proofofconcept.domain.Balance;
import com.rbkmoney.shumaich.proofofconcept.domain.Plan;
import com.rbkmoney.shumaich.proofofconcept.domain.Posting;
import org.junit.Assert;

import java.util.List;

class Utils {
    public static List<Integer> createAccounts() {
        return List.of(1, 2, 3, 4);
    }

    public static Plan createPlan(String planId, OperationType operationType, Integer multiplier) {
        return Plan.builder()
                .planId(planId)
                .operationType(operationType)
                .postings(List.of(
                        createPosting(1, 2, 2800 * multiplier),
                        createPosting(1, 3, 4000 * multiplier),
                        createPosting(4, 1, 80000 * multiplier),
                        createPosting(2, 4, 1760 * multiplier)
                ))
                .build();
    }

    public static Posting createPosting(Integer accFrom, Integer accTo, Integer amount) {
        return Posting.builder()
                .accFrom(accFrom)
                .accTo(accTo)
                .amount(amount)
                .build();
    }

    public static void checkBalance(Balance balance, Integer ownAmount, Integer minAmount, Integer maxAmount) {
        Assert.assertEquals(ownAmount.intValue(), balance.getOwnAmount().get());
        Assert.assertEquals(minAmount.intValue(), balance.getMinAmount().get());
        Assert.assertEquals(maxAmount.intValue(), balance.getMaxAmount().get());
    }

    public static Plan createPlanWithPostings(String planId, OperationType operationType, Posting posting) {
        return Plan.builder()
                .planId(planId)
                .operationType(operationType)
                .postings(List.of(posting))
                .build();
    }
}
