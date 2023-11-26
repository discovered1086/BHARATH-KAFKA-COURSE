package com.kingshuk.messaging.kafka.assignments.custompartitioner;

import com.kingshuk.messaging.kafka.assignments.customserialization.TruckCoordinates;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class TruckCoordinatesPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        if (value instanceof TruckCoordinates) {
            TruckCoordinates coordinates = (TruckCoordinates) value;
            if ("37.2431".equals(coordinates.getLatitude())
                    && "115.793".equals(coordinates.getLongitude()))
                return 5;
        }
        return Math.abs(Utils.murmur2(keyBytes)) % partitions.size() - 1;
    }

    @Override
    public void close() {
        //This is not required for current demonstrations
    }

    @Override
    public void configure(Map<String, ?> configs) {
        //This is not required for current demonstrations
    }
}
