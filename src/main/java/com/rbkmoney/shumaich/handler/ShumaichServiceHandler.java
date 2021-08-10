package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumaich.*;
import com.rbkmoney.shumaich.exception.*;
import com.rbkmoney.shumaich.service.BalanceService;
import com.rbkmoney.shumaich.service.ClockService;
import com.rbkmoney.shumaich.service.RequestRegistrationService;
import com.rbkmoney.shumaich.utils.MdcUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShumaichServiceHandler implements AccounterSrv.Iface {

    public static final String SOME_ERROR_HAPPENED = "Some error happened";
    public static final String POSTINGS_ARE_INVALID = "Postings are invalid!";
    public static final String HOLD_IS_NOT_READ_YET = "Hold has not been read yet";

    private final ClockService clockService;
    private final RequestRegistrationService service;
    private final BalanceService balanceService;

    @Override
    public Clock hold(PostingPlanChange postingPlanChange, Clock clock) throws TException {
        try {
            MdcUtils.setMdc(postingPlanChange);
            log.info("Received hold operation");
            clockService.softCheckClockTimeline(clock);
            return service.registerHold(postingPlanChange);
        } catch (NotReadyException e) {
            log.info(HOLD_IS_NOT_READ_YET, e);
            throw new NotReady();
        } catch (CurrencyInPostingsNotConsistentException |
                AccountsInPostingsAreEqualException |
                AccountsHaveDifferentCurrenciesException e) {
            log.warn(POSTINGS_ARE_INVALID, e);
            throw new InvalidPostingParams();
        } catch (Exception e) {
            log.error(SOME_ERROR_HAPPENED, e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }
    }

    @Override
    public Clock commitPlan(PostingPlan postingPlan, Clock clock) throws TException {
        try {
            MdcUtils.setMdc(postingPlan);
            log.info("Received commit operation");
            clockService.hardCheckClockTimeline(clock);
            return service.registerFinalOp(postingPlan, OperationType.COMMIT);
        } catch (NotReadyException e) {
            log.info(HOLD_IS_NOT_READ_YET, e);
            throw new NotReady();
        } catch (CurrencyInPostingsNotConsistentException |
                AccountsInPostingsAreEqualException |
                AccountsHaveDifferentCurrenciesException |
                HoldChecksumMismatchException e) {
            log.warn(POSTINGS_ARE_INVALID, e);
            throw new InvalidPostingParams();
        } catch (Exception e) {
            log.error(SOME_ERROR_HAPPENED, e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }

    }

    @Override
    public Clock rollbackPlan(PostingPlan postingPlan, Clock clock) throws TException {
        try {
            MdcUtils.setMdc(postingPlan);
            log.info("Received rollback operation");
            clockService.hardCheckClockTimeline(clock);
            return service.registerFinalOp(postingPlan, OperationType.ROLLBACK);
        } catch (NotReadyException e) {
            log.info(HOLD_IS_NOT_READ_YET, e);
            throw new NotReady();
        } catch (CurrencyInPostingsNotConsistentException |
                AccountsInPostingsAreEqualException |
                AccountsHaveDifferentCurrenciesException |
                HoldChecksumMismatchException e) {
            log.warn(POSTINGS_ARE_INVALID, e);
            throw new InvalidPostingParams();
        } catch (Exception e) {
            log.error(SOME_ERROR_HAPPENED, e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }

    }

    @Override
    public Balance getBalanceByID(long accountId, Clock clock) throws TException {
        try {
            MdcUtils.setMdc(accountId);
            clockService.softCheckClockTimeline(clock);
            return balanceService.getBalance(accountId).setClock(clock); //todo добавить настоящий "последний" клок?
        } catch (NotReadyException e) {
            log.info(HOLD_IS_NOT_READ_YET, e);
            throw new NotReady();
        } catch (AccountNotFoundException e) {
            log.info("Account not found", e);
            throw new AccountNotFound(accountId);
        } catch (Exception e) {
            log.error(SOME_ERROR_HAPPENED, e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }
    }

    @Override
    public Account getAccountByID(long accountId, Clock clock) throws TException {
        try {
            MdcUtils.setMdc(accountId);
            clockService.softCheckClockTimeline(clock);
            return balanceService.getAccount(accountId);
        } catch (NotReadyException e) {
            log.info(HOLD_IS_NOT_READ_YET, e);
            throw new NotReady();
        } catch (AccountNotFoundException e) {
            log.info("Account not found", e);
            throw new AccountNotFound(accountId);
        } catch (Exception e) {
            log.error(SOME_ERROR_HAPPENED, e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }
    }
}
