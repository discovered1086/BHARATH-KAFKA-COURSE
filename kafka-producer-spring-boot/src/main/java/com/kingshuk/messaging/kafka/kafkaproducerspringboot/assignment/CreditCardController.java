package com.kingshuk.messaging.kafka.kafkaproducerspringboot.assignment;

import com.kingshuk.messaging.kafka.kafkaproducerspringboot.kafkaclient.UserServiceKafkaObjectProducer;
import com.kingshuk.messaging.kafka.kafkaproducerspringboot.model.UserInfo;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
public class CreditCardController {

    private final CreditCardKafkaProducer creditCardKafkaProducer;

    @PostMapping("/credit-card")
    public String userCreate(@RequestBody CreditCard cardDetails) {
        creditCardKafkaProducer.sendMessage(cardDetails);
        return "success";
    }
}
