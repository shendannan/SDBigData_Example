package consumer;

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * 虽然自动提交offset十分简介便利，但由于其是基于时间提交的，开发人员难以把握
 * offset提交的时机。因此Kafka还提供了手动提交offset的API。
 * 手动提交offset的方法有两种：commitSync（同步提交）和 commitAsync（异步提交）。
 * 两者的相同点是，都会将本次poll的一批数据最高的偏移量提交；
 * 不同点是，commitAsync则没有失败重试机制，故有可能提交失败。
 * 而commitSync阻塞当前线程，一直到提交成功，并且会自动失败重试
 */
public class ManualConsumer {

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
        //3.订阅（多个）主题
        consumer.subscribe(Arrays.asList("first"));
        //4.获取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
            }
            //同步提交，当前线程会阻塞直到 offset 提交成功
//            consumer.commitSync();
            //异步提交
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    System.err.println("Commit failed for" + offsets);
                }
            });
        }
    }
}
