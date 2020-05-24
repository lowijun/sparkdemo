package com.lowi.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区器
 */
public class CustomNumPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        //得到topic的partitions信息
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        //todo 将某些key放入最后一个分区中
        if (key.toString().equals("10000") || key.toString().equals("11111")){
            //放到最后一个分区中
            return numPartitions - 1;
        }
        String strKey = key.toString();
        return strKey.substring(0,3).hashCode() % (numPartitions - 1);
    }
    @Override
    public void close() {
        //todo nothing
    }
    @Override
    public void configure(Map<String, ?> map) {
        //todo nothing
    }
}
