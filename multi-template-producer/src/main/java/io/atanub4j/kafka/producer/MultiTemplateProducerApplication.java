package io.atanub4j.kafka.producer;

import io.atanub4j.kafka.producer.model.Foo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@org.springframework.boot.autoconfigure.SpringBootApplication
@Slf4j
public class MultiTemplateProducerApplication {

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(MultiTemplateProducerApplication.class, args);
    }
    /*
     * Boot will autowire this into the container factory.
     * Configures a DLT recoverer with a single retry after 1 second.
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<String, Object> template) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 1));
    }
    /*
     * Automatic deserialization of JSON messages to Foo objects.
     */
    @Bean
    public RecordMessageConverter converter() {
        return new JsonMessageConverter();
    }
    /*
     * Kafka listener container
     */
    @KafkaListener(id = "fooGroup", topics = "foo-topic")
    public void listen(Foo foo) {
        log.info("Received: " + foo);
        if (foo.getFoo().startsWith("fail")) {
            throw new RuntimeException("failed");
        }
    }
    /*
     * DLT listener container
     */
    @KafkaListener(id = "dltGroup", topics = "foo-topic.DLT")
    public void dltListen(byte[] in) {
        log.info("Received from DLT: " + new String(in));
    }

}
