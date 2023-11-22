package com.kingshuk.messaging.kafka.customserialization;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@EqualsAndHashCode
public class Order implements Serializable {
    private String customerName;
    private String productName;
    private int quantity;
}
