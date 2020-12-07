package sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 打包后放到flume的lib文件夹下后直接按照命令行编写conf文件
 * 调用bin/flume-ng agent -n $agent_name -c conf/ -f your-job-conf.conf使用
 */
public class MySink extends AbstractSink implements Configurable {

    //获取logger对象
    private final Logger logger = LoggerFactory.getLogger(MySink.class);

    //定义全局前缀和后缀
    private String prefix;
    private String suffix;

    @Override
    public void configure(Context context) {
        //读取配置信息给前后缀赋值
        prefix = context.getString("prefix");
        suffix = context.getString("suffix","default");
    }

    /**
     * 1.获取channel
     * 2.从channel获取事务以及数据
     * 3.发送数据
     */
    @Override
    public Status process() {
        //1.定义返回值
        Status status = null;
        //2.获取channel
        Channel channel = getChannel();
        //3.从channel获取事务
        Transaction transaction = channel.getTransaction();
        //4.开启事务
        transaction.begin();
        try {
            //5.从channel获取数据
            Event event = channel.take();
            //6.处理事务
            if(event!=null){
                String body = new String(event.getBody());
                logger.info(prefix + "--" + body + "--" + suffix);
            }
            //7.提交事务
            transaction.commit();
            //8.提交成功，修改状态信息
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            //9.提交事务失败
            transaction.rollback();
            //10.修改状态
            status = Status.BACKOFF;
        } finally {
            //11.关闭事务
            transaction.close();
        }
        return status;
    }
}
