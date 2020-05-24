package com.lowi.kafka.consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OrderConsumer {

    public static void main(String[] args) {
        ExecutorService threadPool = Executors.newFixedThreadPool(20);
        //步骤一：创建消费者
        KafkaConsumer<String,String> consumer = createConsumer();
        //步骤二：指定要消费的主题
        //我们的一个消费者是可以同时消费多个主题
        consumer.subscribe(Arrays.asList("test8"));
        try{
            while (true) {
                //步骤三：去服务端消费数据
                ConsumerRecords<String, String> records = consumer.poll(1000); //超时时间
                //步骤四：对数据进行处理
                //接下来这些就是我们业务逻辑的事
                for (ConsumerRecord<String,String> record: records) {
                    JSONObject order = JSONObject.parseObject(record.toString());
                    threadPool.submit(new OrderTask(order));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KafkaConsumer<String, String> createConsumer() {
        String groupId = "test";
        Properties props = new Properties();
        //指定broker id
        props.put("bootstrap.servers","node1:9092");
        //指定消费组id
        props.put("group.id",groupId);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("heartbeat.interval.ms",3000);
        props.put("session.timeout.ms",10 * 1000);
        props.put("max.poll.interval.ms", 5 * 1000);
        props.put("fetch.max.bytes", 10 * 1024 * 1024);
        props.put("max.poll.records", 1000);
        props.put("connection.max.idle.ms", -1);
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 5 * 1000);
        props.put("auto.offset.reset", "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private static class OrderTask implements Runnable {
        private JSONObject order;
        public OrderTask(JSONObject order) {
            this.order = order;
        }
        @Override
        public void run() {
            //业务处理的代码
            System.out.println("消费者：获取到了数据，对数据进行业务处理....." + order.toString());
        }
    }
}
