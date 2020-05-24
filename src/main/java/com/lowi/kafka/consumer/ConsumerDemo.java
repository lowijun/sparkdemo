package com.lowi.kafka.consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerDemo {
    private  static ExecutorService threadPool = Executors.newFixedThreadPool(20);

    public static void main(String[] args) {
        KafkaConsumer<String,String> consumer = createConsumer();
        consumer.subscribe(Arrays.asList("order-topic"));
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Integer.MAX_VALUE);
                for (ConsumerRecord<String,String> record: records){
                    JSONObject order = JSONObject.parseObject(record.value());
                    threadPool.submit(new CreditManageTask(order));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            consumer.close();
        }
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("heartbeat.interval.ms",1000); //这个尽量时间短一点
        //如果说kafka broker在10秒内感知不到一个consumer心跳
        //就会认为那个consumer挂了，此时会触发rebalance

        props.put("session.timeout.ms", 10 * 1000);
        props.put("max.poll.interval.ms", 30 * 1000); //如果30秒才去执行下一次poll

        //如果说某个consumer挂了，kafka broker感知到了，会触发一个rebalance的操作，就是分配他的分区
        //给其他的consumer来消费，其他的consumer如果要感知到rebalance重新分配分区，就需要通过心跳来感知
        //心跳的间隔一般不要太长，1000，500
        props.put("fetch.max.bytes", 10485760);
        props.put("max.poll.records", 500); //如果消费的吞吐量特别大，此时可以适当提高一些
        props.put("connection.max.idle.ms",-1); //不要回收那个socket连接
        //开启自动提交，只会每隔一段时间去提交一次offset
        //如果每次要重启一下consumer的话，一定会把一些数据重新消费一遍
        props.put("enable.auto.commit", "true");
        //每次自动提交offset的一个时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //每次重启都是从最早的offset开始读取，不是接着上一次
        props.put("auto.offset.reset","earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;

    }

    private static class CreditManageTask implements Runnable {
        private JSONObject order;
        public CreditManageTask(JSONObject order) {
            this.order = order;
        }

        @Override
        public void run() {
            System.out.println("对订单进行积分的维护......" + order.toJSONString());
            // 就可以做一系列的数据库的增删改查的事务操作
        }
    }
}
