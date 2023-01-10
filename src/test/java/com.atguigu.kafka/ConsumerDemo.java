package com.atguigu.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.*;

@SpringBootTest
@Slf4j
public class ConsumerDemo {

    private final static String TOPIC_NAME = "my-topic2";
    private final static String CONSUMER_GROUP_NAME = "myGroup2";

    @Test
    public void simpleSyncConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //        // 是否自动提交offset，默认就是true
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        // 自动提交默认，offset的间隔时间
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅主题列表
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        System.out.println("consumer准备消费------------------------------------------->");
        while (true) {
            /*
             * poll() API 是拉取消息的⻓轮询
             * 默认一次poll 500条消息
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("consumer收到消息：partition = %d,offset = %d, key =%s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    @Test
    public void manualCommitSync() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅主题列表
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        System.out.println("consumer准备消费------------------------------------------->");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            System.out.println("===================每隔5S拉取一次或者满500条消息执行poll,不会一直阻塞==================================");
            // 手动同步提交offset，当前线程会阻塞直到offset提交成功
            // 一般使用同步提交，因为提交之后一般也没有什么逻辑代码了
            if (records.count() > 0) {
                //TODO  处理业务逻辑
                System.out.println("consumer消费的数据size=" + records.count());
//                consumer.commitSync();
            }
        }
    }

    @Test
    public void manualCommitAsync() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅主题列表
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        System.out.println("consumer准备消费------------------------------------------->");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(8000));
            log.info("每隔8S拉取一次或者满500条消息执行poll,会阻塞主线程...");
            if (records.count() > 0 ) {
                // 手动异步提交offset，当前线程提交offset不会阻塞，可以继续处理后面的程序逻辑
                consumer.commitAsync(new OffsetCommitCallback() {
                    @SneakyThrows
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        log.info("执行异步方法onComplete(),不阻塞主线程...");
                        Thread.sleep(2000);
                        if (exception != null) {
                            log.error("Commit failed for {}" , offsets);
                            log.error("Commit failed exception: {}", exception.getMessage());
                        }
                    }
                });
            }
            log.info("main线程正常执行.... {}", Thread.currentThread().getName());
        }
    }

    @Test
    public void seekToBeginning(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //消息回溯消费
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0 )));
        consumer.seek(new TopicPartition(TOPIC_NAME, 0 ), 0 );

        System.out.println("consumer准备消费------------------------------------------->");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("consumer收到消息：partition = %d,offset = %d, key =%s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    @Test
    public void seekOffset() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //指定offset消费
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0 )));
        consumer.seek(new TopicPartition(TOPIC_NAME, 0 ), 3 );

        System.out.println("consumer准备消费------------------------------------------->");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("consumer收到消息：partition = %d,offset = %d, key =%s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    @Test
    public void seekTime() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        List<PartitionInfo> topicPartitions =consumer.partitionsFor(TOPIC_NAME);
        //从 1 小时前开始消费
        long fetchDataTime = new Date().getTime() - 1000 * 60 * 60 ;
        Map<TopicPartition, Long> map = new HashMap<>();
        for (PartitionInfo par : topicPartitions) {
            map.put(new TopicPartition(TOPIC_NAME, par.partition()),fetchDataTime);
        }
        Map<TopicPartition, OffsetAndTimestamp> parMap =consumer.offsetsForTimes(map);
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry :parMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndTimestamp value = entry.getValue();
            if (key == null || value == null) continue;
            Long offset = value.offset();
            System.out.println("==================================================");
            log.info("partition: {} | offset: {}" , key.partition() ,offset);
            System.out.println("==================================================");
            //根据消费里的timestamp确定offset
            if (value != null) {
                consumer.assign(Arrays.asList(key));
                consumer.seek(key, offset);
            }
        }

        System.out.println("consumer准备消费------------------------------------------->");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("consumer收到消息：partition = %d,offset = %d, key =%s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    @Test
    public void seekEarliest() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.33.215:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

//        当消费主题的是一个新的消费组，或者指定offset的消费方式，offset不存在，那么应该如何消费?
//        latest(默认) ：只消费自己启动之后发送到主题的消息
//        earliest：第一次从头开始消费，以后按照消费offset记录继续消费，这个需要区别于consumer.seekToBeginning(每次都从头开始消费)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅主题列表
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        System.out.println("consumer准备消费------------------------------------------->");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("consumer收到消息：partition = %d,offset = %d, key =%s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

}
