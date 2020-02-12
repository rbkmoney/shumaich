package com.rbkmoney.shumaich.proofofconcept;

import com.rbkmoney.shumaich.proofofconcept.domain.Balance;
import com.rbkmoney.shumaich.proofofconcept.domain.Plan;
import com.rbkmoney.shumaich.proofofconcept.domain.PlanTransaction;
import com.rbkmoney.shumaich.proofofconcept.domain.exception.NotReadyException;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Server {

    private static final Integer PARTITIONS_NUM = 3;
    private List<List<PlanTransaction>> partitions = new ArrayList<>();
    private List<Integer> balancesOffsets = new CopyOnWriteArrayList<>();

    private Map<Integer, Balance> balances = new ConcurrentSkipListMap<>();

    private Set<PlanTransaction> readHistory = new CopyOnWriteArraySet<>();

    Server() {
        for (int i = 0; i < PARTITIONS_NUM; i++) {
            partitions.add(new CopyOnWriteArrayList<>());
            balancesOffsets.add(0);
        }
    }

    /**
     * returns Clock of written plan record or later (in concurrent environment)
     */
    public List<Integer> makeOperation(Plan plan) {
        List<PlanTransaction> planTransactions = convertPlanToPlanTransactions(plan);
        writeToPartitions(planTransactions);
        return partitions.stream().map(List::size).collect(Collectors.toList());
    }

    public Balance getBalance(Integer accountId, List<Integer> clock) {
        if (balancesOffsets.get(accountId % PARTITIONS_NUM) < clock.get(accountId % PARTITIONS_NUM))
            throw new NotReadyException();
        log.info("balances:{}", balances);
        return balances.get(accountId % PARTITIONS_NUM);
    }

    public Balance getBalance(Integer accountId) {
        this.calculateBalancesToLatest();
        log.info("balances:{}", balances);
        return balances.get(accountId % PARTITIONS_NUM);
    }

    public synchronized void calculateBalancesTo(List<Integer> clock) {
        for (int partitionIndex = 0; partitionIndex < partitions.size(); partitionIndex++) {
            if (clock.get(partitionIndex) <= balancesOffsets.get(partitionIndex))
                continue;

            try {
                partitions.get(partitionIndex)
                        .subList(balancesOffsets.get(partitionIndex), clock.get(partitionIndex))
                        .stream()
                        .filter(planTransaction -> !readHistory.contains(planTransaction))
                        .peek(planTransaction -> readHistory.add(planTransaction))
                        .forEach(this::calculateBalance);
                balancesOffsets.set(partitionIndex, clock.get(partitionIndex));
            } catch (Throwable ex) {
                System.out.println();
            }

        }
    }

    public void calculateBalancesToLatest() {
        calculateBalancesTo(partitions.stream().map(List::size).collect(Collectors.toList()));
    }

    private void calculateBalance(PlanTransaction planTransaction) {
        Balance balance = balances.get(planTransaction.getAccount());
        Integer amount = planTransaction.getAmountDiff();
        switch (planTransaction.getOperationType()) {
            case HOLD:
                if (amount > 0) balance.getMaxAmount().getAndAdd(amount);
                else balance.getMinAmount().getAndAdd(amount);
                break;
            case COMMIT:
                balance.getOwnAmount().getAndAdd(amount);
                // добавление min/max amount захолдированной суммы (по протоколу)
                if (amount > 0) balance.getMinAmount().addAndGet(amount);
                else balance.getMaxAmount().addAndGet(amount);
                break;
            case ROLLBACK:
                if (amount > 0) balance.getMaxAmount().getAndAdd(-amount);
                else balance.getMinAmount().addAndGet(-amount);
                break;
        }
        balances.put(planTransaction.getAccount(), balance);
    }

    public void initAcc(Integer accNumber, Balance initialBalance) {
        balances.put(accNumber, initialBalance);
    }

    public void initAccs(List<Integer> accNumbers) {
        for (Integer accNumber : accNumbers) {
            balances.put(accNumber, new Balance(0, 0, 0));
        }
    }

    private List<PlanTransaction> convertPlanToPlanTransactions(Plan plan) {
        Integer total = plan.getPostings().size() * 2;
        PrimitiveIterator.OfInt seqSupplier = IntStream.range(1, total + 1).iterator();
        return plan.getPostings().stream().flatMap(posting -> List.of(
                new PlanTransaction(
                        plan.getPlanId(),
                        plan.getOperationType(),
                        seqSupplier.next(),
                        total,
                        -posting.getAmount(),
                        posting.getAccFrom()
                ),
                new PlanTransaction(
                        plan.getPlanId(),
                        plan.getOperationType(),
                        seqSupplier.next(),
                        total,
                        posting.getAmount(),
                        posting.getAccTo()
                )
        ).stream()).collect(Collectors.toList());
    }

    private void writeToPartitions(List<PlanTransaction> planTransactions) {
        for (PlanTransaction planTransaction : planTransactions) {
            partitions.get(planTransaction.getAccount() % PARTITIONS_NUM).add(planTransaction);
        }
    }

}
