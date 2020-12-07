package consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        //1.创建Kafka消费者的配置信息，以下列出了较全的配置信息，大多数采用默认配置
        Properties properties = new Properties();
        //2.指定连接的Kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        //3.指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        //4.开启自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //5.自动提交的延迟
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //6.Key,value的序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //可选：是否从头开始，等同于--from—beginning,但仅当offset过期或消费者组第一次获取数据时才生效
        //默认是latest，即不从头开始获取数据
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //7.创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //8.订阅（多个）主题
        consumer.subscribe(Arrays.asList("first","second"));
        //9.获取数据
        while (true) {
            //注意ConsumerRecords和ConsumerRecord的关系
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
            }
        }
        //这里可以不用关闭连接，程序走完了自动关闭，因此需要死循环不停地接收数据
    }
}
