package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        // 2. 给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.30.8.58:9092, 172.30.32.67:9092");

        // key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 3. 创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

//        for (int i = 0; i < 5; i++) {
//            // 指定数据发送到 1 号分区
//            kafkaProducer.send(new ProducerRecord<>("first67",  0,"","atguigu " + i), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata,  Exception e) {
//                    if (e == null){
//                        System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition());
//                    }else {
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }

        for (int i = 0; i < 5; i++) {
            // 指定key 数据发送到 某分区
            kafkaProducer.send(new ProducerRecord<>("first67","g","atguigu " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata,  Exception e) {
                    if (e == null){
                        System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition());
                    }else {
                        e.printStackTrace();
                    }
                }
            });
        }

        kafkaProducer.close();

    }
}