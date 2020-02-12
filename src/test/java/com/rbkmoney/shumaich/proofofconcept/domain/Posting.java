package com.rbkmoney.shumaich.proofofconcept.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Posting {
    Integer accFrom;
    Integer accTo;
    Integer amount;
}
