package producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 不带回调函数的普通生产者，包含同步传输和异步传输，列出了较全的参数
 */
public class MyProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建Kafka生产者的配置信息，以下列出了较全的配置信息，大多数采用默认配置
        Properties properties = new Properties();
        //2.指定连接的Kafka集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        //3.ACK应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //4.重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);
        //5.批次大小，16KB
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //6.等待时间，1ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //7.RecordAccumulator缓冲区大小，32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //8.Key,value的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //9.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //10.发送数据，不带回调函数
        for (int i = 0; i < 100; i++) {
            //异步发送，注意这里我们指定了key为i，以key的哈希值分区
            producer.send(new ProducerRecord<>("first",
                    Integer.toString(i), "data--" + i));
            //同步发送（很少用），一条消息发送之后，会阻塞当前线程，直至返回ack。
//            producer.send(new ProducerRecord<String, String>("first",
//                    Integer.toString(i), Integer.toString(i))).get();
        }
        //11.关闭资源,一定要关闭 producer，这样才会调用 interceptor 的 close 方法
        producer.close();
    }
}
