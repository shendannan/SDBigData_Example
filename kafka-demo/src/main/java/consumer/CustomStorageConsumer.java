package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 数据存储在外部数据库时，例如 MySQL，需要用户自定义存储 offset。
 * offset 的维护是相当繁琐的，因为需要考虑到消费者分区的重新分配 Rebalace。
 * 消费者发生 Rebalance 之后，每个消费者消费的分区就会发生变化。因此消费者要首先
 * 获取到自己被重新分配到的分区，并且定位到每个分区最近提交的 offset 位置继续消费
 */
public class CustomStorageConsumer {

    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        //1.创建Kafka消费者的配置信息
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "master:9092");
        properties.put("group.id", "test");
        //注意，此处关闭自动提交offset
        properties.put("enable.auto.commit", "false");
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //2.创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //3.订阅（多个）主题，注意此处有区别，需要借助到ConsumerRebalanceListener
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            //该方法会在 Rebalance 之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            //该方法会在 Rebalance 之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    //定位到最近提交的 offset 位置继续消费
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        //4.获取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            commitOffset(currentOffset);//异步提交
        }
    }

    //获取某分区的最新 offset
    private static long getOffset(TopicPartition partition) {
        //TODO:需要根据所选的 offset 存储系统自行实现
        return 0;
    }

    //提交该消费者所有分区的 offset，需要根据所选的 offset 存储系统自行实现
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
        //TODO:需要根据所选的 offset 存储系统自行实现
    }
}
