package io.atanub4j.kafka.producer.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@Configuration
public class MultiTemplateProducerConfiguration {

    @Bean
    public Supplier<List<String>> bootstrapServersSupplier() {
        return () -> List.of("localhost:9092");
    }
    /*
     * Common producer configurations
     */
    @Bean
    public Map<String, Object> baseProducerConfigs(Supplier<List<String>> bootstrapServersSupplier) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersSupplier.get());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
    /*
     * Producer factory that can be shared among multiple KafkaTemplates
     */
    @Bean
    public ProducerFactory<?, ?> multiTypeProducerFactory() {
        return new DefaultKafkaProducerFactory<>(baseProducerConfigs(bootstrapServersSupplier()));
    }
    /*
     * this kafka template uses base producer configs with default String serializer for both key and value
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplateString(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
    /*
     * this kafka template overrides base producer configs with Json serializer for value
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplateObject(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(
                producerFactory,
                Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerializer"));
    }
    /*
     * this kafka template overrides base producer configs with ByteArray serializer for value and Integer serializer for key
     */
    @Bean
    public KafkaTemplate<Integer, byte[]> kafkaTemplateByteArray(ProducerFactory<Integer, byte[]> producerFactory) {
        return new KafkaTemplate<>(
                producerFactory,
                Map.of(
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer"));
    }
    /*
     * this kafka template overrides base producer configs with producer configs tuned for high throughput
     */
    @Bean
    public KafkaTemplate<String, JsonNode> kafkaTemplateHighThroughput(ProducerFactory<String, JsonNode> producerFactory) {
        return new KafkaTemplate<>(
                producerFactory,
                Map.of(
                        // Throughput Tuning
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerializer",
                        ProducerConfig.BATCH_SIZE_CONFIG, 65536,
                        ProducerConfig.LINGER_MS_CONFIG, 20,
                        ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4",
                        ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864));
    }

    /*
     * this kafka template has a global producer listener to log success / failure of message send
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplateWithProducerListener(ProducerFactory<String, String> producerFactory) {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setDefaultTopic("foo-topic");
        kafkaTemplate.setProducerListener(new ProducerListener<>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
                log.info("GlobalSuccessMessage:: {}, sent to topic:: {}, partition:: {}, offset:: {}",
                        producerRecord.value(), recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset());
            }
            @Override
            public void onError(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata, Exception exception) {
                log.error("GlobalErrorMessage:: {}, sent to topic:: {}, partition:: {}, offset:: {}. Exception: {}",
                        producerRecord.value(), recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), exception.getMessage());
            }
        });
        return kafkaTemplate;
    }
}