package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 采用自定义分区器的生产者
 */
public class PartitionProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建Kafka生产者的配置信息，以下只列出了必要的配置参数
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //注意，此处添加自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"partitioner.Mypartitioner");

        //2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //3.发送数据，带回调函数
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("first",
                    Integer.toString(i), "data--" + i), (metadata, exception) -> {
                if (exception == null) {
                    //如果 Exception 为 null，说明消息发送成功
                    System.out.println(metadata.partition() + "--" + metadata.offset());
                } else {
                    //消息发送失败会自动重试，不需要我们在回调函数中手动重试
                    exception.printStackTrace();
                }
            });
        }
        //4.关闭资源
        producer.close();
    }

}
