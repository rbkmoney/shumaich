package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.converter.PostingPlanOperationToOperationLogListConverter;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.helpers.IdempotentTestHandler;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.awaitility.Awaitility.await;

@Slf4j
@ContextConfiguration(classes = {OperationLogHandlerServiceIntegrationTest.Config.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class OperationLogHandlerServiceIntegrationTest extends IntegrationTestBase {

    @SpyBean
    KafkaTemplate<String, OperationLog> operationLogKafkaTemplate;

    @Autowired
    IdempotentTestHandler handler;

    @Autowired
    private PostingPlanOperationToOperationLogListConverter converter;

    @Autowired
    TopicConsumptionManager<String, OperationLog> operationLogTopicConsumptionManager;

    @Test
    public void successEventPropagation() throws InterruptedException, ExecutionException {
        String testPlanId = "plan1";
        PostingPlanOperation plan = TestData.postingPlanOperation(testPlanId);
        sendPlanToPartition(plan);

        int totalPostings = plan.getPostingBatches().stream()
                .mapToInt(postingBatch -> postingBatch.getPostings().size()).sum();

        await().untilAsserted(() ->
                Assert.assertEquals(totalPostings * 2, handler.countReceivedRecords(testPlanId).intValue()));
    }

//
//    @Test
//    @Ignore // cейчас flush() нет, возможно верну и тест верну
//    public void kafkaTemplateFlushErrorRetried() throws InterruptedException {
//        Mockito.doThrow(RuntimeException.class)
//                .doCallRealMethod()
//                .when(operationLogKafkaTemplate).flush();
//
//        String testPlanId = "plan3";
//        PostingPlanOperation plan = TestData.postingPlanOperation(testPlanId);
//        sendPlanToPartition(plan);
//
//        Thread.sleep(6000);
//
//        int totalPostings = plan.getPostingBatches().stream()
//                .mapToInt(postingBatch -> postingBatch.getPostings().size()).sum();
//
//        Assert.assertEquals(totalPostings * 2,
//                handler.countReceivedRecords(testPlanId).intValue());
//    }


    private void sendPlanToPartition(PostingPlanOperation plan) {
        List<OperationLog> operationLogs = converter.convert(plan);
        for (OperationLog operationLog : operationLogs) {
            sendOperationLogToPartition(operationLog);
        }

    }


    private void sendOperationLogToPartition(OperationLog operationLog) {
        operationLogKafkaTemplate.sendDefault(operationLog.getAccount().getId(), operationLog);
    }

    @Configuration
    public static class Config {

        @Bean
        @Primary
        Handler<OperationLog> operationLogHandler() {
            return new IdempotentTestHandler();
        }

    }
}
