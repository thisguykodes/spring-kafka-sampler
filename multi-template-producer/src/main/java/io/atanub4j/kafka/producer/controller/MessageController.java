package io.atanub4j.kafka.producer.controller;

import io.atanub4j.kafka.producer.model.Foo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/kafka-producer")
@Slf4j
public class MessageController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateString;
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplateObject;
    @Autowired
    private KafkaTemplate<Integer, byte[]> kafkaTemplateByteArray;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateWithProducerListener;

    /*
     * Completable future approach to log success / failure of message send
     * per-message solution
     */
    private static void logMessage(SendResult<?, ?> result, Throwable ex) {
        if (null != ex) {
            log.error("Error sending message. Exception: {}", ex.getMessage());
        } else {
            log.info("Message:: {}, sent to topic:: {}, partition:: {}, offset:: {}",
                    result.getProducerRecord().value(), result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        }
    }

    @PostMapping("/send/string/{message}")
    public void sendStringMessageToKafkaTopic(@PathVariable String message) {
        kafkaTemplateString
            .send("foo-topic", "1", message)
            .whenComplete(MessageController::logMessage);
    }

    @PostMapping("/send/pojo/{message}")
    public void sendPojoMessageToKafkaTopic(@PathVariable String message) {
        kafkaTemplateObject
            .send("foo-topic", "1", new Foo(message))
            .whenComplete(MessageController::logMessage);
    }

    @PostMapping("/send/byte/{message}")
    public void sendByteArrayMessageToKafkaTopic(@PathVariable String message) {
        kafkaTemplateByteArray
                .send("foo-topic", 1, message.getBytes(StandardCharsets.UTF_8))
                .whenComplete(MessageController::logMessage);
    }

    @PostMapping("/send/with-producer-listener/{message}")
    public void sendMessageWithProducerListener(@PathVariable String message) {
        kafkaTemplateWithProducerListener.sendDefault(message);
    }
}
