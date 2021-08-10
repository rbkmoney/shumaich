package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.domain.PostingBatch;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PostingBatchDamselToPostingBatchConverter {

    private final PostingDamselToPostingConverter converter;

    public PostingBatch convert(com.rbkmoney.damsel.shumaich.PostingBatch postingBatchDamsel) {
        return PostingBatch.builder()
                .id(postingBatchDamsel.getId())
                .postings(postingBatchDamsel.getPostings()
                        .stream()
                        .map(converter::convert)
                        .collect(Collectors.toList()))
                .build();
    }

}
