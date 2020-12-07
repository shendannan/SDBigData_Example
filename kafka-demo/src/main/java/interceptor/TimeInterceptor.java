package interceptor;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 添加时间戳拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        //1.取出数据
        String value = record.value();
        //2.创建一个新的 record，把时间戳写入消息体的最前部
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), System.currentTimeMillis() + "," + value);
    }
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {
    }

}
