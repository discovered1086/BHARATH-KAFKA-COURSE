package com.kingshuk.messaging.kafka.assignments.customdeserialization;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@ToString
@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class TruckCoordinates implements Serializable {
    private String truckId;
    private String latitude;
    private String longitude;
}
