package io.atanub4j.simpleproducer.controller;

import io.atanub4j.simpleproducer.model.Foo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/simple-producer")
public class SimpleProducerController {
    @Autowired
    KafkaTemplate<Object, Object> template;

    @PostMapping("/messages/{message}")
    public void sendSimpleMessageToKafkaTopic(@PathVariable String message) {
        template.send("foo-topic", new Foo(message));
    }

}
