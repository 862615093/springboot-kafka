package com.atguigu.kafka.producer;

import cn.hutool.json.JSONUtil;
import com.atguigu.kafka.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SyncProducer {

    private final static String TOPIC_NAME = "my-replicated-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092,172.30.33.215:9093,172.30.33.215:9094");
        //把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        Order order = new Order(3L, "hello,kafka33...");
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, order.getId() + "", JSONUtil.toJsonStr(order));
        RecordMetadata metadata = producer.send(producerRecord).get();
        //=====阻塞======= 同步方式发送消息结果：
        //topic-my-replicated-topic|partition-1|offset-0
        //topic-my-replicated-topic|partition-0|offset-0
        //topic-my-replicated-topic|partition-1|offset-1
        //topic-my-replicated-topic|partition-1|offset-2
        System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-" + metadata.partition() + "|offset-" + metadata.offset());
    }
}
