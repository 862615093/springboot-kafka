package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 生产经验——生产者如何提高吞吐量
 *
 */
public class CustomProducerParameters {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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