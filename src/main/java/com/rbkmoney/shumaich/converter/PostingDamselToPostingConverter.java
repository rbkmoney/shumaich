package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.Account;
import com.rbkmoney.shumaich.domain.Posting;
import org.springframework.stereotype.Component;

@Component
public class PostingDamselToPostingConverter {

    public Posting convert(com.rbkmoney.damsel.shumaich.Posting postingDamsel) {
        return Posting.builder()
                .amount(postingDamsel.getAmount())
                .currencySymbolicCode(postingDamsel.getCurrencySymbolicCode())
                .description(postingDamsel.getDescription())
                .fromAccount(new Account(
                        postingDamsel.getFromAccount().getId(),
                        postingDamsel.getFromAccount().getCurrencySymbolicCode()
                ))
                .toAccount(new Account(
                        postingDamsel.getToAccount().getId(),
                        postingDamsel.getToAccount().getCurrencySymbolicCode()
                ))
                .build();
    }

}
