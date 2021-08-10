package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.kafka.handler.Handler;
import com.rbkmoney.shumaich.service.KafkaOffsetService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class SimpleTopicConsumer<K, V> implements Runnable {

    private final Map<String, Object> consumerProps;
    private final List<TopicPartition> assignedPartitions;
    private final KafkaOffsetService kafkaOffsetService;
    private final Handler<K, V> handler;
    private final Long pollingTimeout;
    private volatile boolean alive = true;
    private KafkaConsumer<K, V> consumer;

    public static <K, V> SimpleTopicConsumer<K, V> of(SimpleTopicConsumer<K, V> otherConsumer) {
        return new SimpleTopicConsumer<>(
                otherConsumer.consumerProps,
                otherConsumer.assignedPartitions,
                otherConsumer.kafkaOffsetService,
                otherConsumer.handler,
                otherConsumer.pollingTimeout
        );
    }

    public boolean isAlive() {
        return alive && !Thread.currentThread().isInterrupted();
    }

    @Override
    public void run() {
        try {
            initConsumer();

            while (isAlive()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollingTimeout));
                if (records.count() > 0) {
                    handler.handle(records);
                    saveOffsetsAndSeek(records);
                }
            }

        } catch (WakeupException e) {
            log.info("Wakeup call occurred", e);
        } catch (InterruptException e) { //kafka exception, not java.lang.
            log.info("Thread was interrupted");
        } catch (Exception e) {
            log.error("Error during Kafka polling", e);
        } finally {
            consumer.close();
            alive = false;
        }
    }

    private void initConsumer() {
        log.debug("Initializing consumer for topic and partitions: {}", assignedPartitions);

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.assign(assignedPartitions);
        List<KafkaOffset> kafkaOffsets = kafkaOffsetService.loadOffsets(assignedPartitions);
        kafkaOffsets.forEach(kafkaOffset -> consumer.seek(kafkaOffset.getTopicPartition(), kafkaOffset.getOffset()));

        log.debug("Initialized consumer for topic and partitions: {}", assignedPartitions);
    }

    private void saveOffsetsAndSeek(ConsumerRecords<K, V> records) {

        List<KafkaOffset> offsets = records.partitions().stream()
                .map(topicPartition -> getLatestKafkaOffset(records, topicPartition))
                .collect(Collectors.toList());

        kafkaOffsetService.saveOffsets(offsets);

        offsets.forEach(offset -> consumer.seek(offset.getTopicPartition(), offset.getOffset()));
    }

    private KafkaOffset getLatestKafkaOffset(ConsumerRecords<K, V> records, TopicPartition topicPartition) {
        List<ConsumerRecord<K, V>> recordsForPartition = records.records(topicPartition);

        long lastOffset = recordsForPartition.get(recordsForPartition.size() - 1).offset();
        return new KafkaOffset(topicPartition, lastOffset + 1); // +1 to not repeat read of last message
    }

}
