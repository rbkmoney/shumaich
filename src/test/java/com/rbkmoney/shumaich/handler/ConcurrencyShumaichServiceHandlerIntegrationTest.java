package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumaich.Account;
import com.rbkmoney.damsel.shumaich.Balance;
import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.damsel.shumaich.PostingPlanChange;
import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.dao.BalanceDao;
import com.rbkmoney.shumaich.dao.PlanDao;
import com.rbkmoney.shumaich.helpers.HellgateClientExecutor;
import com.rbkmoney.shumaich.helpers.HoldPlansExecutor;
import com.rbkmoney.shumaich.helpers.PostingGenerator;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import com.rbkmoney.shumaich.service.BalanceService;
import com.rbkmoney.shumaich.service.PlanService;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.TransactionDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.shaded.com.google.common.util.concurrent.Futures;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.rbkmoney.shumaich.helpers.TestData.*;
import static org.junit.Assert.assertEquals;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@Ignore
public class ConcurrencyShumaichServiceHandlerIntegrationTest extends IntegrationTestBase {

    public static final Long TEST_CASE_FIRST = 10000000000L;
    public static final Long TEST_CASE_SECOND = 20000000000L;
    private static final int ITERATIONS = 10;
    private static final int OPERATIONS = 15000;
    private static final int THREAD_NUM = 16;
    private static final long HOLD_AMOUNT = 100;
    @Autowired
    ShumaichServiceHandler serviceHandler;

    @Autowired
    TopicConsumptionManager<Long, OperationLog> operationLogTopicConsumptionManager;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    BalanceDao balanceDao;

    @Autowired
    BalanceService balanceService;

    @Autowired
    PlanService planService;

    @Autowired
    PlanDao planDao;

    @Autowired
    TransactionDB rocksDB;

    RetryTemplate retryTemplate = getRetryTemplate();
    private ExecutorService executorService;

    @NotNull
    private RetryTemplate getRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new AlwaysRetryPolicy());
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(100L);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    @Test
    public void concurrentHoldsConsistencyTest() throws InterruptedException {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            executorService = Executors.newFixedThreadPool(THREAD_NUM);

            List<Future<Map.Entry<String, Balance>>> futureList = new ArrayList<>();

            for (int operation = 0; operation < OPERATIONS; operation++) {
                PostingPlanChange postingPlanChange = PostingGenerator.createPostingPlanChange(
                        TEST_CASE_FIRST + "_iteration" + iteration + "_operation" + operation,
                        TEST_CASE_FIRST + iteration + PROVIDER_ACC,
                        TEST_CASE_FIRST + iteration + SYSTEM_ACC,
                        TEST_CASE_FIRST + iteration + MERCHANT_ACC,
                        HOLD_AMOUNT
                );

                futureList.add(executorService.submit(new HoldPlansExecutor(
                                serviceHandler,
                                postingPlanChange,
                                retryTemplate,
                                TEST_CASE_FIRST + iteration + MERCHANT_ACC
                        )
                ));
            }

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);

            Balance balance = futureList.stream()
                    .map(Futures::getUnchecked)
                    .map(Map.Entry::getValue)
                    .min(Comparator.comparing(Balance::getMinAvailableAmount))
                    .orElseThrow(RuntimeException::new);

            long expectedBalance = -HOLD_AMOUNT * OPERATIONS;
            assertEquals("Wrong balance after holds", expectedBalance, balance.getMinAvailableAmount());
        }
    }

    @Test
    public void concurrentHellgateSimulationTest() throws InterruptedException {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            executorService = Executors.newFixedThreadPool(THREAD_NUM);

            List<Future<Map.Entry<String, Balance>>> futureList = new ArrayList<>();

            initBalance(TEST_CASE_SECOND + iteration + MERCHANT_ACC);

            for (int operation = 0; operation < OPERATIONS; operation += 2) {
                futureList.add(executorService.submit(new HellgateClientExecutor(
                                serviceHandler,
                                PostingGenerator.createPostingPlanChangeTwoAccs(
                                        TEST_CASE_SECOND + iteration + "_operation" + operation,
                                        TEST_CASE_SECOND + iteration + SYSTEM_ACC,
                                        TEST_CASE_SECOND + iteration + MERCHANT_ACC,
                                        HOLD_AMOUNT
                                ),
                                retryTemplate,
                                TEST_CASE_SECOND + iteration + SYSTEM_ACC
                        )
                ));
                futureList.add(executorService.submit(new HellgateClientExecutor(
                                serviceHandler,
                                PostingGenerator.createPostingPlanChangeTwoAccs(
                                        TEST_CASE_SECOND + iteration + "_operation" + (operation + 1),
                                        TEST_CASE_SECOND + iteration + MERCHANT_ACC,
                                        TEST_CASE_SECOND + iteration + SYSTEM_ACC,
                                        HOLD_AMOUNT
                                ),
                                retryTemplate,
                                TEST_CASE_SECOND + iteration + MERCHANT_ACC
                        )
                ));
            }

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);

            checkBalancesAreNotNegative(futureList);
        }
    }

    private void initBalance(Long account) {
        balanceService.createNewBalance(new Account(account, "RUB"));
        balanceService.proceedHold(new OperationLog()
                .setPlanId("test")
                .setAccount(new com.rbkmoney.damsel.shumaich.Account(account, "RUB"))
                .setAmountWithSign(HOLD_AMOUNT)
                .setCurrencySymbolicCode("RUB")
                .setSequenceId(1L)
                .setPlanOperationsCount(1L)
                .setBatchId(1L)
                .setCreationTimeMs(Instant.EPOCH.toEpochMilli())
                .setOperationType(com.rbkmoney.damsel.shumaich.OperationType.HOLD));
        balanceService.proceedFinalOp(new OperationLog()
                .setAccount(new com.rbkmoney.damsel.shumaich.Account(account, "RUB"))
                .setAmountWithSign(HOLD_AMOUNT)
                .setCurrencySymbolicCode("RUB")
                .setSequenceId(1L)
                .setPlanOperationsCount(1L)
                .setBatchId(1L)
                .setCreationTimeMs(Instant.EPOCH.toEpochMilli())
                .setOperationType(com.rbkmoney.damsel.shumaich.OperationType.COMMIT));
    }

    private void checkBalancesAreNotNegative(List<Future<Map.Entry<String, Balance>>> futureList) {
        Assert.assertFalse(futureList.stream()
                .map(Futures::getUnchecked)
                .map(Map.Entry::getValue)
                .anyMatch(balance -> balance.getOwnAmount() < 0));
    }

}
