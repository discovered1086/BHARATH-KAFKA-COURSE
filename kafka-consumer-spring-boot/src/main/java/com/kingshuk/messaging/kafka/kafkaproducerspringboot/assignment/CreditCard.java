package com.kingshuk.messaging.kafka.kafkaproducerspringboot.assignment;


import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class CreditCard {
    private String fullName;
    private String cardNumber;
    private String expirationDate;
    private String cvc;
}
