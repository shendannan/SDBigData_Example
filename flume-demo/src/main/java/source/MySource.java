package source;

import org.apache.flume.Context;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * 官网上有示例代码
 * 打包后放到flume的lib文件夹下后直接按照命令行编写conf文件
 * 调用bin/flume-ng agent -n $agent_name -c conf/ -f your-job-conf.conf使用
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
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
     * 1.接收数据（for循环制造数据，实际可以对接JDBC）
     * 2.封装为事件
     * 3.将事件传给channel
     */
    @Override
    public Status process() {
        Status status = null;
        try {
            //1.接收数据
            for(int i = 0;i<5;i++){
                //2.构建事件对象
                SimpleEvent event = new SimpleEvent();
                //3.给事件设置值
                event.setBody((prefix + "--" + i + "--" + suffix).getBytes());
                //4.将事件传给channel
                getChannelProcessor().processEvent(event);

                status = Status.READY;
            }
        } catch (Exception e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //返回结果
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
