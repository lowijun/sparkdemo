package com.lowi.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * 测试自定义分区器
 */
public class CustomNumPartitionerProducer {
    private static final String[] nums_str =
            new String[]{"10000", "10000", "11111", "13700000003", "13700000004",
            "10000", "15500000006", "11111", "15500000008", "17600000009", "10000","17600000011"};

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        //设置分区器
        props.put("partitioner.class","com.lowi.kafka.producer.CustomNumPartitioner");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建一个producer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int count = 0;
        int length = nums_str.length;

        while (count < 10) {
            Random random = new Random();
            String strNum = nums_str[random.nextInt(length)];
            ProducerRecord<String, String> record = new ProducerRecord<>("dev3-topic001", strNum, strNum);
            RecordMetadata metadata = producer.send(record).get();
            String result = "phonenum [" + record.value() + "] has been sent to partition " + metadata.partition();
            System.out.println(result);
            Thread.sleep(500);
            count ++ ;
        }
        producer.close();

    }
}
