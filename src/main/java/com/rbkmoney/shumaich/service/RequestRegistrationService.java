package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumaich.Clock;
import com.rbkmoney.damsel.shumaich.PostingPlan;
import com.rbkmoney.damsel.shumaich.PostingPlanChange;
import com.rbkmoney.shumaich.converter.PostingPlanChangeToPostingPlanOperationConverter;
import com.rbkmoney.shumaich.converter.PostingPlanToPostingPlanOperationConverter;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.domain.ValidationStatus;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RequestRegistrationService {

    private final PostingPlanChangeToPostingPlanOperationConverter holdConverter;
    private final PostingPlanToPostingPlanOperationConverter finalOpConverter;
    private final WriterService writerService;
    private final ClockService clockService;
    private final ValidationService validationService;

    public Clock registerHold(PostingPlanChange postingPlanChange) {
        validationService.validatePostings(postingPlanChange);

        PostingPlanOperation postingPlanOperation = holdConverter.convert(postingPlanChange);
        return writeToTopic(postingPlanOperation);
    }

    public Clock registerFinalOp(PostingPlan postingPlan, OperationType operationType) {
        validationService.validatePostings(postingPlan);
        PostingPlanOperation postingPlanOperation = finalOpConverter.convert(postingPlan, operationType);
        ValidationStatus validationStatus = validationService.validateFinalOp(postingPlanOperation);
        if (validationStatus != null)  {
            log.info("Hold does not exist, maybe it is already cleared");
            postingPlanOperation.setValidationStatus(validationStatus);
        }

        return writeToTopic(postingPlanOperation);
    }

    private Clock writeToTopic(PostingPlanOperation postingPlanOperation) {
        List<RecordMetadata> partitionsMetadata = writerService.write(postingPlanOperation);
        String clock = clockService.formClock(partitionsMetadata);
        return Clock.vector(VectorClockSerde.serialize(clock));
    }

}
