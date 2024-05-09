package com.kingshuk.messaging.kafka.kafkaproducerspringboot.web;

import com.kingshuk.messaging.kafka.kafkaproducerspringboot.kafkaclient.UserServiceKafkaObjectProducer;
import com.kingshuk.messaging.kafka.kafkaproducerspringboot.model.UserInfo;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
public class UserController {

    private final UserServiceKafkaObjectProducer userServiceKafkaProducer;

    @PostMapping("/users")
    public String userCreate(@RequestBody UserInfo user) {
        userServiceKafkaProducer.sendMessage(user);
        return "success";
    }
}
