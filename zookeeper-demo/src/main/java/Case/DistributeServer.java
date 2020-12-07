package Case;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * 监听服务器节点动态上下线案例
 * 某分布式系统中，主节点可以有多台，可以动态上下线，任意一台客户端都能实时感知
 * 到主节点服务器的上下线。
 */
public class DistributeServer {
    private static final String connectString = "master:2181,slave1:2181,slave2:2181,slave3:2181";
    private static final int sessionTimeout = 2000;
    private ZooKeeper zk = null;

    // 1.创建到zk的客户端连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new
                Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                    }
                });
    }

    // 2.注册服务器节点
    public void registServer(String hostname) throws Exception {
        String parentNode = "/servers";
        // 带序号的临时节点允许覆盖
        String create = zk.create(parentNode + "/server",
                hostname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online " + create);
    }

    // 3.业务功能
    public void business(String hostname) throws Exception {
        System.out.println(hostname + " is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        //参数args[0]为hostname主机名
        //1.连接zk集群
        DistributeServer server = new DistributeServer();
        server.getConnect();
        //2.利用zk连接注册服务器信息
        server.registServer(args[0]);
        //3.启动业务功能
        server.business(args[0]);
    }
}
