package com.kingshuk.messaging.kafka.kafkaproducerspringboot.model;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class UserInfo {
    private String name;
    private int age;
    private String email;
    private String phone;
}
