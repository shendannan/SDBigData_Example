package producer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class InterceptorProducer {
    public static void main(String[] args) throws Exception {
        //1.设置配置信息
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "master:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        //2.构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("interceptor.TimeInterceptor");
        interceptors.add("interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        //3.创建生产者对象
        Producer<String, String> producer = new KafkaProducer<>(properties);
        //4.发送消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first", "message" + i);
            producer.send(record);
        }
        //5.关闭 producer
        producer.close();
    }
}
