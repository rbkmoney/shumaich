package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.Posting;
import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PostingDamselToPostingConverterTest {

    PostingDamselToPostingConverter converter = new PostingDamselToPostingConverter();

    @Test
    public void conversion() {
        Posting postingDamsel = TestData.postingDamsel();
        var posting = converter.convert(postingDamsel);

        assertEquals(postingDamsel.getAmount(), posting.getAmount().longValue());
        assertEquals(
                postingDamsel.getFromAccount().getCurrencySymbolicCode(),
                posting.getFromAccount().getCurrencySymbolicCode()
        );
        assertEquals(postingDamsel.getFromAccount().getId(), posting.getFromAccount().getId());
        assertEquals(
                postingDamsel.getToAccount().getCurrencySymbolicCode(),
                posting.getToAccount().getCurrencySymbolicCode()
        );
        assertEquals(postingDamsel.getToAccount().getId(), posting.getToAccount().getId());
        assertEquals(postingDamsel.getCurrencySymbolicCode(), posting.getCurrencySymbolicCode());
        assertEquals(postingDamsel.description, posting.getDescription());
    }

}
