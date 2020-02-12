package com.rbkmoney.shumaich.proofofconcept.domain;

import com.rbkmoney.shumaich.domain.OperationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Plan {
    String planId;
    OperationType operationType;
    List<Posting> postings;
}
