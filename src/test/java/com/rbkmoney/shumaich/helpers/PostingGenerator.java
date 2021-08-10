package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumaich.*;

import java.util.ArrayList;
import java.util.List;

public class PostingGenerator {

    public static final long BATCH_ID = 1L;


    public static PostingBatch createBatch(Long providerAcc, Long systemAcc, Long merchantAcc, Long batchId) {
        PostingBatch batch = new PostingBatch();
        batch.setId(batchId);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymbolicCode("RUB")
                .setAmount(100)
                .setFromAccount(new Account(providerAcc, "RUB"))
                .setToAccount(new Account(merchantAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymbolicCode("RUB")
                .setAmount(3)
                .setFromAccount(new Account(merchantAcc, "RUB"))
                .setToAccount(new Account(systemAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymbolicCode("RUB")
                .setAmount(1)
                .setFromAccount(new Account(systemAcc, "RUB"))
                .setToAccount(new Account(providerAcc, "RUB"))
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }

    public static PostingBatch createBatch(Long providerAcc, Long systemAcc, Long merchantAcc) {
        return createBatch(providerAcc, systemAcc, merchantAcc, BATCH_ID);
    }

    public static PostingBatch createBatchWithFixedAmount(
            Long providerAcc,
            Long systemAcc,
            Long merchantAcc,
            Long amount) {
        PostingBatch batch = new PostingBatch();
        batch.setId(BATCH_ID);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymbolicCode("RUB")
                .setAmount(amount)
                .setFromAccount(new Account(providerAcc, "RUB"))
                .setToAccount(new Account(merchantAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymbolicCode("RUB")
                .setAmount(amount)
                .setFromAccount(new Account(merchantAcc, "RUB"))
                .setToAccount(new Account(systemAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymbolicCode("RUB")
                .setAmount(amount)
                .setFromAccount(new Account(systemAcc, "RUB"))
                .setToAccount(new Account(providerAcc, "RUB"))
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }

    public static PostingPlanChange createPostingPlanChange(
            String planId,
            Long providerAcc,
            Long systemAcc,
            Long merchantAcc) {
        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);

        return new PostingPlanChange()
                .setBatch(batch)
                .setId(planId)
                .setCreationTime("2016-03-22T06:12:27Z");
    }

    public static PostingPlanChange createPostingPlanChange(
            String planId,
            Long providerAcc,
            Long systemAcc,
            Long merchantAcc,
            Long amount) {
        PostingBatch batch = PostingGenerator.createBatchWithFixedAmount(providerAcc, systemAcc, merchantAcc, amount);

        return new PostingPlanChange()
                .setBatch(batch)
                .setCreationTime("2016-03-22T06:12:27Z")
                .setId(planId);
    }

    public static PostingPlan createPostingPlan(String planId, Long providerAcc, Long systemAcc, Long merchantAcc) {
        return new PostingPlan()
                .setBatchList(List.of(
                        PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, BATCH_ID)))
                .setId(planId)
                .setCreationTime("2016-03-22T06:12:27Z");
    }

    public static Posting createPosting() {
        return new Posting()
                .setCurrencySymbolicCode("RUB")
                .setAmount(1)
                .setFromAccount(new Account(1, "RUB"))
                .setToAccount(new Account(2, "RUB"))
                .setDescription("test");
    }

    public static PostingPlanChange createPostingPlanChangeTwoAccs(
            String planId,
            Long firstAcc,
            Long secondAcc,
            long amount) {
        return new PostingPlanChange()
                .setId(planId)
                .setBatch(new PostingBatch()
                        .setId(BATCH_ID)
                        .setPostings(List.of(new Posting()
                                .setCurrencySymbolicCode("RUB")
                                .setAmount(amount)
                                .setFromAccount(new Account(firstAcc, "RUB"))
                                .setToAccount(new Account(secondAcc, "RUB"))
                                .setDescription("qwe"))));
    }
}
