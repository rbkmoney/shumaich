package com.rbkmoney.shumaich.proofofconcept.domain;

import com.rbkmoney.shumaich.domain.OperationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PlanTransaction {
    String planId;
    OperationType operationType;
    Integer seq;
    Integer total;
    Integer amountDiff;
    Integer account;
}
