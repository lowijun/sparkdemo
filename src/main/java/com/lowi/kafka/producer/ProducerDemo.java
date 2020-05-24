package com.lowi.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 生产者
 */
public class ProducerDemo {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        //这里可以配置几台broker即可，会自动从broker去拉取元数据进行缓存
        props.put("bootstrap.servers","node1:9092,node2:9092,node3:9092");
        //这个就是负责把发送的key从字符串序列化为字节数组
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //这个就是负责把发送的时间message从字符串序列化为字节数组
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks","-1");
        props.put("retries",3);
        props.put("batch.size",323840);
        props.put("linger.ms",10);
        props.put("buffer.memory",33554432);
        props.put("max.block.ms",3000);
        //创建一个producer实例：线程资源，跟各个broker建立socket连接资源
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");

        //异步发送模式
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null) {
                    //消息发送成功
                    System.out.println("消息发送成功");
                } else {
                    //消息发送失败，需要重新发送
                }
            }
        });
        Thread.sleep(10 * 1000);

        //这里是同步发送模式
//        producer.send(record).get();
        //需要一直等待后续一系列的步骤都做完，发送消息之后
        //有了消息回应返回，才会执行下面的方法，才退出生产者
        producer.close();
    }

}
