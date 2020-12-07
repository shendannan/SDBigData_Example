package producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 采用回调函数的生产者,回调函数会在 producer 收到 ack 时调用，为异步调用.
 */
public class CallBackProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建Kafka生产者的配置信息，以下只列出了必要的配置参数
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //3.发送数据，带回调函数
        for (int i = 0; i < 100; i++) {
            //回调函数，该方法会在 Producer收到ack时调用，为异步调用，用lamda表达式来写
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
