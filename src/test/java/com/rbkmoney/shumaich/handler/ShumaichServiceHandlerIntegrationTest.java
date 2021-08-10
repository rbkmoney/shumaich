package com.rbkmoney.shumaich.handler;

import com.google.common.primitives.Longs;
import com.rbkmoney.damsel.shumaich.*;
import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.dao.BalanceDao;
import com.rbkmoney.shumaich.dao.PlanDao;
import com.rbkmoney.shumaich.domain.Balance;
import com.rbkmoney.shumaich.exception.NotReadyException;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.helpers.TestUtils;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import com.rbkmoney.shumaich.service.BalanceService;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;

import java.net.URI;
import java.util.Map;

import static com.rbkmoney.shumaich.helpers.TestData.*;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Kafka doesn't clear data between tests, so u need unique ID's for test runs
 */
@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class ShumaichServiceHandlerIntegrationTest extends IntegrationTestBase {

    @Autowired
    ShumaichServiceHandler handler;

    @Autowired
    BalanceDao balanceDao;

    @Autowired
    PlanDao planDao;

    @Autowired
    TransactionDB rocksDB;

    @SpyBean
    BalanceService balanceService;

    @Autowired
    TopicConsumptionManager<Long, OperationLog> operationLogTopicConsumptionManager;

    @Before
    public void clearDbData() throws RocksDBException {
        rocksDB.delete(planDao.getColumnFamilyHandle(), (PLAN_ID + "_HOLD").getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan1_HOLD".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan2_HOLD".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), (PLAN_ID + "_COMMIT").getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan1_COMMIT".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan2_COMMIT".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), (PLAN_ID + "_ROLLBACK").getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan1_ROLLBACK".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan2_ROLLBACK".getBytes());
        rocksDB.delete(balanceDao.getColumnFamilyHandle(), Longs.toByteArray(MERCHANT_ACC));
        rocksDB.delete(balanceDao.getColumnFamilyHandle(), Longs.toByteArray(SYSTEM_ACC));
        rocksDB.delete(balanceDao.getColumnFamilyHandle(), Longs.toByteArray(PROVIDER_ACC));
    }

    @Test
    public void holdSuccess() throws TException {

        handler.hold(TestData.postingPlanChange(), null);

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            assertNotNull(balance);
            assertEquals(0, balance.getAmount().intValue());
            assertEquals(-3, balance.getMinAmount().intValue());
            assertEquals(100, balance.getMaxAmount().intValue());
        });

    }

    @Test
    public void holdIdempotency() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        handler.hold(plan1, null);
        handler.hold(plan1, null);
        handler.hold(plan1, null);

        PostingPlanChange plan2 = TestData.postingPlanChange();
        plan2.setId("plan2");
        handler.hold(plan2, null);
        handler.hold(plan2, null);
        handler.hold(plan2, null);

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);
            assertNotNull(balance);
            assertEquals(0, balance.getAmount().intValue());
            assertEquals(-6, balance.getMinAmount().intValue());
            assertEquals(200, balance.getMaxAmount().intValue());
        });
    }

    @Test
    public void holdWithClockRightInTime() throws TException {

        PostingPlanChange plan1 = TestData.postingPlanChange();
        Clock clock = handler.hold(plan1, null);

        //second batch
        plan1.getBatch().setId(2L);

        given().ignoreExceptions().await()
                .until(() -> handler.hold(plan1, clock), notNullValue());


        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            assertNotNull(balance);
            assertEquals(0, balance.getAmount().intValue());
            assertEquals(-6, balance.getMinAmount().intValue());
            assertEquals(200, balance.getMaxAmount().intValue());
        });
    }

    @Test(expected = NotReady.class)
    public void holdWithClockTooEarly() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        Clock clock = handler.hold(plan1, null);

        //second batch
        plan1.getBatch().setId(2L);

        clock = TestUtils.moveClockFurther(clock, Map.of(3L, 10L));
        handler.hold(plan1, clock);
    }

    @Test(expected = InvalidPostingParams.class)
    public void holdInvalidPostingsDifferentCurrency() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        plan1.getBatch().getPostings().get(0).setCurrencySymbolicCode("USD");
        handler.hold(plan1, null);
    }

    @Test(expected = InvalidPostingParams.class)
    public void holdInvalidPostingsEqualAccs() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        plan1.getBatch().getPostings().get(0).setFromAccount(plan1.getBatch().getPostings().get(0).getToAccount());
        handler.hold(plan1, null);
    }

    @Test(expected = InvalidPostingParams.class)
    public void holdInvalidPostingsDifferentAccountCurrencies() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        plan1.getBatch().getPostings().get(0).getFromAccount().setCurrencySymbolicCode("USD");
        handler.hold(plan1, null);
    }

    @Test
    public void commitSuccess() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        await().until(() -> handler.commitPlan(TestData.postingPlan(), holdClock), notNullValue());

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            assertNotNull(balance);
            assertEquals(97, balance.getAmount().intValue());
            assertEquals(97, balance.getMinAmount().intValue());
            assertEquals(97, balance.getMaxAmount().intValue());
        });
    }

    @Test
    public void commitIdempotency() throws TException {
        Clock holdClock = handler.hold(postingPlanChange("unique"), null);

        //wait for hold to be consumed
        await().until(() -> handler.commitPlan(TestData.postingPlan("unique"), holdClock), notNullValue());

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);
            assertNotNull(balance);
            assertEquals(97, balance.getAmount().intValue());
            assertEquals(97, balance.getMinAmount().intValue());
            assertEquals(97, balance.getMaxAmount().intValue());
        });

        handler.commitPlan(TestData.postingPlan(), holdClock);

        //second commit shouldn't be proceeded
        verify(balanceService, times(6))
                .proceedHold(argThat(operationLog -> operationLog.getPlanId().contains("unique")));
        verify(balanceService, times(6))
                .proceedFinalOp(argThat(operationLog -> operationLog.getPlanId().contains("unique")));
    }

    @Test
    public void rollbackSuccess() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        await().until(() -> handler.rollbackPlan(TestData.postingPlan(), holdClock), notNullValue());

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            assertNotNull(balance);
            assertEquals(0, balance.getAmount().intValue());
            assertEquals(0, balance.getMinAmount().intValue());
            assertEquals(0, balance.getMaxAmount().intValue());
        });
    }

    @Test
    public void rollbackIdempotency() throws TException {
        Clock holdClock = handler.hold(postingPlanChange("12345"), null);

        //wait for hold to be consumed
        await().until(() -> handler.rollbackPlan(TestData.postingPlan("12345"), holdClock), notNullValue());

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            assertNotNull(balance);
            assertEquals(0, balance.getAmount().intValue());
            assertEquals(0, balance.getMinAmount().intValue());
            assertEquals(0, balance.getMaxAmount().intValue());
        });

        handler.rollbackPlan(TestData.postingPlan(), holdClock);

        //second rollback shouldn't be proceeded
        verify(balanceService, times(6))
                .proceedHold(argThat(operationLog -> operationLog.getPlanId().contains("12345")));
        verify(balanceService, times(6))
                .proceedFinalOp(argThat(operationLog -> operationLog.getPlanId().contains("12345")));
    }

    @Test(expected = InvalidPostingParams.class)
    public void finalOperationChecksumMismatch() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        PostingPlan postingPlan = postingPlan();
        Posting posting = postingPlan.getBatchList().get(0).getPostings().get(0);
        posting.setAmount(posting.getAmount() + 1);

        //wait for hold to be consumed
        await().ignoreExceptionsInstanceOf(NotReadyException.class)
                .until(() -> handler.rollbackPlan(postingPlan, holdClock), notNullValue());
    }

    @Test
    public void finalOperationHoldNotExist() throws TException {
        Clock fakeClock = handler.hold(postingPlanChange("notKekPlan"), null);

        PostingPlan postingPlan = postingPlan();
        postingPlan.setId("kekPlan");

        //wait for hold to be consumed
        await().ignoreExceptionsInstanceOf(NotReadyException.class)
                .until(() -> handler.rollbackPlan(postingPlan, fakeClock), notNullValue());

        //second commit shouldn't be proceeded
        verify(balanceService, times(6)).proceedHold(argThat(operationLog -> operationLog.getPlanId()
                .contains("notKekPlan")));
        verify(balanceService, times(0)).proceedFinalOp(argThat(operationLog -> operationLog.getPlanId()
                .contains("kekPlan")));
    }

    @Test
    public void finalOperationBatchNotExist() throws TException {
        Clock fakeClock = handler.hold(postingPlanChange("kektus"), null);

        PostingPlan postingPlan = postingPlan();
        postingPlan.getBatchList().get(0).setId(999L);

        //wait for hold to be consumed
        await().ignoreExceptionsInstanceOf(NotReadyException.class)
                .until(() -> handler.rollbackPlan(postingPlan, fakeClock), notNullValue());

        //second commit shouldn't be proceeded
        verify(balanceService, times(6))
                .proceedHold(argThat(operationLog -> operationLog.getPlanId().contains("kektus")));
        verify(balanceService, times(0))
                .proceedFinalOp(argThat(operationLog -> operationLog.getPlanId().contains("kektus")));
    }

    @Test
    public void findAccountById() throws TException {
        handler.hold(postingPlanChange(), null);

        await().untilAsserted(() -> {
            final Account account = handler.getAccountByID(SYSTEM_ACC, null);

            assertEquals(SYSTEM_ACC.longValue(), account.getId());
            assertEquals("RUB", account.getCurrencySymbolicCode());
        });
    }

    @Test(expected = AccountNotFound.class)
    public void accountNotFound() throws TException {
        handler.getAccountByID(SYSTEM_ACC, null);
    }

    @Test(expected = NotReady.class)
    public void accountNotReady() throws TException {
        final Clock clock = handler.hold(postingPlanChange(), null);
        handler.getAccountByID(SYSTEM_ACC, TestUtils.moveClockFurther(clock, Map.of(1L, 100L, 2L, 100L, 3L, 100L)));
    }

    @Test
    public void balanceById() throws TException {
        handler.hold(postingPlanChange(), null);

        await().untilAsserted(() -> {
            final com.rbkmoney.damsel.shumaich.Balance balanceByID = handler.getBalanceByID(MERCHANT_ACC, null);

            assertEquals(MERCHANT_ACC.longValue(), balanceByID.getId());
            assertEquals(-3, balanceByID.min_available_amount);
        });
    }

    @Test(expected = AccountNotFound.class)
    public void balanceNotFound() throws TException {
        handler.getBalanceByID(MERCHANT_ACC, null);
    }

    @Test(expected = NotReady.class)
    public void balanceNotReady() throws TException {
        final Clock clock = handler.hold(postingPlanChange(), null);
        handler.getBalanceByID(MERCHANT_ACC, TestUtils.moveClockFurther(clock, Map.of(1L, 10L, 2L, 10L, 3L, 10L)));
    }

    @Test
    public void woodyTest() throws TException {
        final AccounterSrv.Iface shumaichIface = new THSpawnClientBuilder()
                .withAddress(URI.create("http://localhost:8022/shumaich"))
                .build(AccounterSrv.Iface.class);

        shumaichIface.hold(postingPlanChange(), null);

        await().untilAsserted(() -> {
            final com.rbkmoney.damsel.shumaich.Balance balanceByID = handler.getBalanceByID(MERCHANT_ACC, null);

            assertEquals(MERCHANT_ACC.longValue(), balanceByID.getId());
            assertEquals(-3, balanceByID.min_available_amount);
        });
    }
}
