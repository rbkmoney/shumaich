package com.rbkmoney.shumaich.proofofconcept.domain;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

@Data
public class Balance {

    AtomicInteger ownAmount;
    AtomicInteger minAmount;
    AtomicInteger maxAmount;

    public Balance(Integer own, Integer min, Integer max) {
        this.ownAmount = new AtomicInteger(own);
        this.minAmount = new AtomicInteger(min);
        this.maxAmount = new AtomicInteger(max);
    }
}
