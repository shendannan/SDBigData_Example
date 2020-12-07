package Case;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {

    //1.初始化配置，注意不要有空格，zk连接设置成全局变量
    private static final String connectString = "master:2181,slave1:2181,slave2:2181,slave3:2181";
    private static final int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    //2.创建ZK客户端，连接服务器
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(event.getType() + "--" +
                        event.getPath());
                //再次启动监听
                try {
                    zkClient.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //3.创建子节点
    @Test
    public void create() throws Exception {
        //参数1：要创建的节点的路径；参数2：节点数据；参数3：节点权限；参数4：节点的类型（持久、临时、是否带序号）
        String nodeCreated = zkClient.create("/new_path",
                "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    //4.获取子节点并监听节点变化
    @Test
    public void getChildren() throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        //延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    //5.判断znode是否存在
    @Test
    public void exist() throws Exception {
        Stat stat = zkClient.exists("/eclipse", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }

}
