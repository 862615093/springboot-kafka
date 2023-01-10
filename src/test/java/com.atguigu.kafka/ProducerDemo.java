package com.atguigu.kafka;

import cn.hutool.json.JSONUtil;
import com.atguigu.kafka.entity.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootTest
public class ProducerDemo {

    private final static String TOPIC_NAME = "my-topic2";

    @Test
    public void simpleProducer() throws ExecutionException, InterruptedException {
        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        // 2. 给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092,172.30.33.215:9093,172.30.33.215:9094");
        // key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 3. 创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 4. 调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
            RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, "simple msg: " + i)).get();
            System.out.println("producer发送消息：" + "topic:" + metadata.topic() +
                    "| partition:" + metadata.partition() + "| offset:" + metadata.offset());
        }
        // 5. 关闭资源
        kafkaProducer.close();
    }

    @Test
    public void simpleSyncProducer() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        //把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        acks=0： 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。性能最高，但是最容易丢消息。
//        acks=1： 至少要等待leader已经成功将数据写入本地log，但是不需要等待所有follower是否成功写入。就可以继续发送下一条消息。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
//        acks=-1或all： 需要等待 min.insync.replicas(默认为 1 ，推荐配置大于等于2) 这个参数配置的副本个数都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 8; i < 9; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, i + "", "hello, consumer:" + i);
            RecordMetadata metadata = producer.send(producerRecord).get();
            System.out.println("producer发送消息：" + "topic:" + metadata.topic() +
                    "| partition:" + metadata.partition() + "| offset:" + metadata.offset());
        }
    }

    @Test
    public void syncProducerAck() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092,172.30.33.215:9093,172.30.33.215:9094");
        //把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        acks=0： 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。性能最高，但是最容易丢消息。
//        acks=1： 至少要等待leader已经成功将数据写入本地log，但是不需要等待所有follower是否成功写入。就可以继续发送下一条消息。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
//        acks=-1或all： 需要等待 min.insync.replicas(默认为 1 ，推荐配置大于等于2) 这个参数配置的副本个数都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        Producer<String, String> producer = new KafkaProducer<>(props);

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

    //生产经验——生产者如何提高吞吐量
    @Test
    public void improvePerformance() throws ExecutionException, InterruptedException {
        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        // 2. 给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.32.67:9092");

        // key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // batch.size：批次大小，默认 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // linger.ms：等待时间，默认 0 注意： 同步发送才有效，异步发送无效
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);

        // RecordAccumulator：缓冲区大小，默认 32M：buffer.memory
//        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);

        // compression.type：压缩，默认 none，可配置值 gzip、snappy、
//        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        // 3. 创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        System.out.println("-------------------------->");

        // 4. 调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
            RecordMetadata first67 = kafkaProducer.send(new ProducerRecord<>("first67", "jjj " + i), (metadata, exception) -> {
                System.out.println("=========================>");
                if (exception == null) {
                    // 没有异常,输出信息到控制台
                    System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition());
                } else {
                    // 出现异常打印
                    exception.printStackTrace();
                }
            }).get();
        }

        // 5. 关闭资源
        kafkaProducer.close();
    }
}
