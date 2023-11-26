package com.kingshuk.messaging.kafka.custompartitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class OrderPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        if(key.equals("MacBook Pro")){
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
