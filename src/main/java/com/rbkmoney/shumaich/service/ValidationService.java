package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumaich.*;
import com.rbkmoney.shumaich.domain.Plan;
import com.rbkmoney.shumaich.domain.PlanBatch;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.exception.AccountsHaveDifferentCurrenciesException;
import com.rbkmoney.shumaich.exception.AccountsInPostingsAreEqualException;
import com.rbkmoney.shumaich.exception.CurrencyInPostingsNotConsistentException;
import com.rbkmoney.shumaich.exception.HoldChecksumMismatchException;
import com.rbkmoney.shumaich.utils.HashUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ValidationService {

    private final PlanService planService;

    public void validatePostings(PostingPlanChange postingPlanChange) {
        validatePostings(postingPlanChange.getBatch().getPostings());
    }

    public void validatePostings(PostingPlan postingPlan) {
        for (PostingBatch postingBatch : postingPlan.getBatchList()) {
            validatePostings(postingBatch.getPostings());
        }
    }

    private void validatePostings(List<Posting> postings) {
        String currencySymCode = postings.get(0).getCurrencySymbolicCode();
        for (Posting posting : postings) {
            if (!posting.getCurrencySymbolicCode().equals(currencySymCode)) {
                throw new CurrencyInPostingsNotConsistentException();
            }
            if (posting.getFromAccount().getId() == (posting.getToAccount().getId())) {
                throw new AccountsInPostingsAreEqualException();
            }
            if (!posting.getFromAccount()
                    .getCurrencySymbolicCode()
                    .equals(posting.getToAccount().getCurrencySymbolicCode())) {
                throw new AccountsHaveDifferentCurrenciesException();
            }
        }
    }

    public ValidationError validateFinalOp(PostingPlanOperation postingPlanOperation) {
        Plan plan = planService.getPlan(postingPlanOperation.getPlanId(), OperationType.HOLD);
        return validatePreviousHold(plan, postingPlanOperation);
    }

    public ValidationError validatePreviousHold(Plan plan, PostingPlanOperation postingPlanOperation) {
        if (plan == null) {
            return ValidationError.HOLD_NOT_EXIST;
        }

        for (com.rbkmoney.shumaich.domain.PostingBatch postingBatch : postingPlanOperation.getPostingBatches()) {
            PlanBatch storedBatch = plan.getBatch(postingBatch.getId());
            if (storedBatch == null) {
                return ValidationError.HOLD_NOT_EXIST;
            }
            if (!HashUtils.areHashesEqual(postingBatch.getPostings(), storedBatch.getBatchHash())) {
                throw new HoldChecksumMismatchException();
            }
        }

        return null;
    }
}
