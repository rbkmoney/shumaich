package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OperationLog {
    private String planId;
    private Long batchId;
    private OperationType operationType;
    private Long account;
    private Long amountWithSign;
    private String currencySymbolicCode;
    private String description;
    //todo remove?
    private Instant creationTime;
    private Integer sequence;
    private Integer total;
}
