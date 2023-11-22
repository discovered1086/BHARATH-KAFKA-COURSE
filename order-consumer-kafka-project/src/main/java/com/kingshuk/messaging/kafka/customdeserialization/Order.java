package com.kingshuk.messaging.kafka.customdeserialization;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@ToString
@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class Order implements Serializable {
    private String customerName;
    private String productName;
    private int quantity;
}
