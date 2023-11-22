package com.kingshuk.messaging.kafka.assignments.customserialization;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@EqualsAndHashCode
public class TruckCoordinates implements Serializable {
    private String truckId;
    private String latitude;
    private String longitude;
}
