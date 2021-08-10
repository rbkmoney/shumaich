package com.rbkmoney.shumaich;


import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.kafka.handler.Handler;
import com.rbkmoney.shumaich.service.KafkaOffsetService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.rbkmoney.shumaich.helpers.TestData.OPERATION_LOG_TOPIC;
import static com.rbkmoney.shumaich.helpers.TestData.TEST_TOPIC;
import static org.mockito.ArgumentMatchers.any;

@Slf4j
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(classes = ShumaichApplication.class)
@ContextConfiguration(initializers = IntegrationTestBase.Initializer.class)
public abstract class IntegrationTestBase {

    public static final EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(1, true, 10,
            TEST_TOPIC, OPERATION_LOG_TOPIC
    );
    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    static {
        kafka.before();
    }

    @Autowired
    protected AdminClient kafkaAdminClient;
    @SpyBean
    protected KafkaOffsetService kafkaOffsetService;

    protected List<TopicPartitionInfo> getTopicPartitions(String topicName)
            throws InterruptedException, ExecutionException {
        return kafkaAdminClient.describeTopics(List.of(topicName))
                .values()
                .get(topicName)
                .get()
                .partitions();
    }

    protected void setInitialOffsets(int partitionNumber, Long initialOffsets, String topicName)
            throws InterruptedException, ExecutionException {
        List<TopicPartitionInfo> partitions = getTopicPartitions(topicName);

        kafkaOffsetService.saveOffsets(partitions.stream()
                .filter(partition -> partition.partition() == partitionNumber)
                .map(topicPartitionInfo -> TestData.kafkaOffset(
                        topicName,
                        topicPartitionInfo.partition(),
                        initialOffsets
                ))
                .collect(Collectors.toList()));

    }

    protected void checkOffsets(int partitionNumber, Long expectedOffset, String topicName)
            throws ExecutionException, InterruptedException {
        List<TopicPartitionInfo> partitions = getTopicPartitions(topicName);

        List<KafkaOffset> kafkaOffsets = kafkaOffsetService.loadOffsets(partitions.stream()
                .filter(partition -> partition.partition() == partitionNumber)
                .map(topicPartitionInfo -> new TopicPartition(topicName, topicPartitionInfo.partition()))
                .collect(Collectors.toList())
        );

        Assert.assertTrue(
                kafkaOffsets.toString(),
                kafkaOffsets.stream().anyMatch(kafkaOffset -> kafkaOffset.getOffset().equals(expectedOffset))
        );
    }

    protected <K, T> void registerReceivedMessages(
            int partitionNumber,
            AtomicInteger receivedRecordsSize,
            Handler<K, T> handler) {
        Mockito.doAnswer(invocation -> {
            // As we can't clean records from topics in test - we just parallelize tests by partitions
            ConsumerRecords<K, T> consumerRecords = (ConsumerRecords<K, T>) invocation.getArguments()[0];
            Optional<TopicPartition> wantedPartition = consumerRecords.partitions().stream()
                    .filter(topicPartition -> topicPartition.partition() == partitionNumber)
                    .findFirst();
            wantedPartition.ifPresent(topicPartition ->
                    receivedRecordsSize.addAndGet(consumerRecords.records(topicPartition).size())
            );
            return null;
        })
                .when(handler)
                .handle(any());
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @SneakyThrows
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of(
                            "kafka.bootstrap-servers=" + kafka.getEmbeddedKafka().getBrokersAsString(),
                            "rocksdb.name=test",
                            "rocksdb.dir=" + folder.newFolder()
                    )
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }


}
