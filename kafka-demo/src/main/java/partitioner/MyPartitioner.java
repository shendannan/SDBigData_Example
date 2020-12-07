package partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器，可参考默认的分区器DefaultPartitioner.java
 */
public class MyPartitioner implements Partitioner {

    /**
     * 根据给定记录计算分区
     * @param s topic 主题名
     * @param o key 键 可为null
     * @param bytes 键的序列化 可为null
     * @param o1 value 值 可为null
     * @param bytes1 值的序列化 可为null
     * @param cluster 当前集群元数据
     * @return 分区号
     */
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //可以按照key或value分区，根据业务逻辑调整
        Integer integer = cluster.partitionCountForTopic(s);//查看可用的分区
        return o.toString().hashCode() % integer;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
