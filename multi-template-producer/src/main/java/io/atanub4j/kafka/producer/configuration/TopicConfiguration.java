package io.atanub4j.kafka.producer.configuration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@Configuration
public class TopicConfiguration {
    @Bean
    KafkaAdmin admin(Supplier<String> bootstrapServersSupplier) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersSupplier);
        return new KafkaAdmin(configs);
    }

    @Bean
    KafkaAdmin.NewTopics allTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("foo-topic").partitions(1).replicas(1).build(),
                TopicBuilder.name("foo-topic-dlt").partitions(1).replicas(1).build()
        );
    }

}
