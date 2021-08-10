package com.rbkmoney.shumaich.config;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import com.rbkmoney.shumaich.kafka.handler.Handler;
import com.rbkmoney.shumaich.kafka.serde.OperationLogDeserializer;
import com.rbkmoney.shumaich.service.KafkaOffsetService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {

    private static final String EARLIEST = "earliest";
    private final KafkaSslProperties kafkaSslProperties;
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.topics.partitions-per-thread}")
    private Integer partitionsPerThread;
    @Value("${kafka.topics.polling-timeout}")
    private Long pollingTimeout;
    @Value("${kafka.topics.operation-log-name}")
    private String operationLogTopicName;

    private Map<String, Object> consumerConfig() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        configureSsl(props, kafkaSslProperties);

        return props;
    }


    private Map<String, Object> producerConfig() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        configureSsl(props, kafkaSslProperties);

        return props;
    }

    @Bean
    @DependsOn(value = "rocksDB")
    public KafkaTemplate<Long, OperationLog> operationLogKafkaTemplate() {
        Map<String, Object> configs = producerConfig();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ThriftSerializer.class);
        KafkaTemplate<Long, OperationLog> kafkaTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(configs), true);
        kafkaTemplate.setDefaultTopic(operationLogTopicName);
        return kafkaTemplate;
    }

    @Bean
    AdminClient kafkaAdminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        configureSsl(props, kafkaSslProperties);

        return AdminClient.create(props);
    }

    @Bean
    @DependsOn("rocksDB")
    public TopicConsumptionManager<Long, OperationLog> operationLogTopicConsumptionManager(
            AdminClient kafkaAdminClient,
            KafkaOffsetService kafkaOffsetService,
            Handler<Long, OperationLog> handler) throws ExecutionException, InterruptedException {
        Map<String, Object> consumerProps = consumerConfig();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OperationLogDeserializer.class);

        TopicDescription topicDescription = kafkaAdminClient
                .describeTopics(List.of(operationLogTopicName))
                .values()
                .get(operationLogTopicName)
                .get();

        return new TopicConsumptionManager<>(
                topicDescription,
                partitionsPerThread,
                consumerProps,
                kafkaOffsetService,
                handler,
                pollingTimeout
        );
    }

    private void configureSsl(Map<String, Object> props, KafkaSslProperties kafkaSslProperties) {
        if (kafkaSslProperties.isEnabled()) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
            props.put(
                    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                    new File(kafkaSslProperties.getTrustStoreLocation()).getAbsolutePath()
            );
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaSslProperties.getTrustStorePassword());
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, kafkaSslProperties.getKeyStoreType());
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, kafkaSslProperties.getTrustStoreType());
            props.put(
                    SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                    new File(kafkaSslProperties.getKeyStoreLocation()).getAbsolutePath()
            );
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, kafkaSslProperties.getKeyStorePassword());
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, kafkaSslProperties.getKeyPassword());
        }
    }

}
