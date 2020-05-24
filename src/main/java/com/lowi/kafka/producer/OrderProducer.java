package com.lowi.kafka.producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) throws InterruptedException {
        //todo 步骤一：创建生产者
        KafkaProducer<String,String> producer = createProducer();
        //步骤二：创建小系统
        JSONObject order = createRecord();
        //topic key value
        ProducerRecord<String, String> record = new ProducerRecord<>("test8", order.getString("userId"), order.toString());
        /**
         * 如果发送消息，消息不指定key，那么我们发送的这些消息，会被轮询的发送到不同的分区
         *
         * 如果指定key。发送消息的时候，客户端会根据这个key计算出来一个hash值
         * 根据这个hash值会把消息发送到对应的分区里面
         */

        //kafka发送数据有两种方式：
        //1.异步方式
        //这里是异步方式的模式
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    //发送消息成功
                    System.out.println("消息发送成功");
                } else {
                    //消息发送失败，需要重新发送
                }
            }
        });
        Thread.sleep(10 * 1000);

        //第二种方式：同步发送模式
//        producer.send(record).get();
        //一直等待后续一系列的步骤做完，发送消息之后
        //有了消息回应返回，这个方法才会退出来
        producer.close();
    }

    private static JSONObject createRecord() {
        JSONObject order = new JSONObject();
        order.put("userId", 12344);
        order.put("amount", 100.0);
        order.put("OPERATION", "pay");
        return order;
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        //这里配置几台broker即可，自动从broker去拉取元数据进行缓存
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        //这就是负责把发送的key从字符串序列化为字节数组
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //负责把发送的实际的massage从字符串序列化为字节数组
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("buffer.memory", 33554432); //默认是32M
        props.put("compression.type", "lz4"); //压缩方式：lz4
        props.put("batch.size", 32768); //32kb
        props.put("linger.ms", 100);
        props.put("retries", 10); //5 10
        props.put("retry.backoff.ms", 300);
        props.put("request.required.acks", "1");
        //todo 创建一个producer实例：线程资源，跟各个broker建立socket连接资源
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }
}
