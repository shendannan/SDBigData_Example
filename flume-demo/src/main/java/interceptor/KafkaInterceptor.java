package interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 实现按照kafka topic数据分类
 * 打包后放到flume的lib文件夹下后直接按照命令行编写conf文件
 * 调用bin/flume-ng agent -n $agent_name -c conf/ -f your-job-conf.conf使用
 */
public class KafkaInterceptor implements Interceptor {

    // 声明一个存放事件的集合
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        //初始化
        addHeaderEvents = new ArrayList<>();
    }

    //单个事件拦截
    @Override
    public Event intercept(Event event) {
        //1.获取事件中的头信息
        Map<String, String> headers = event.getHeaders();
        //2.获取事件中的body信息
        String body = new String(event.getBody());
        //3.根据body中是否有hello来决定添加怎样的头信息
        //TODO:按照官方文档，header为topic的会自动读取其值作为数据实际流向的topic
        // 实验中，拦截器没有生效，还是全部导入到kafkasink里指定的topic中，原因不明
        // 此外，如果kafkasink不指定topic，会导入到default-flume-topic主题内

        //直接在这里写死topic也没有效果
//        headers.put("topic","first");
        if (body.contains("hello")){
            //4.添加头信息，key为topic,value为指定的topic名
            headers.put("topic","first");
        }else{
            headers.put("topic","second");
        }
        return event;
    }

    //批量事件拦截
    @Override
    public List<Event> intercept(List<Event> list) {
        //1.清空集合
        addHeaderEvents.clear();
        //2.遍历events
        for(Event event:list){
            //3.给每个事件添加头信息
            addHeaderEvents.add(intercept(event));
        }
        //4.返回结果
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    //这个类不要忘了，类名可以自己定，用于构建拦截器
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new KafkaInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}