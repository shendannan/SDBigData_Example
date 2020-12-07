package Case;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 监听服务器节点动态上下线案例
 * 某分布式系统中，主节点可以有多台，可以动态上下线，任意一台客户端都能实时感知
 * 到主节点服务器的上下线。
 * 注意要先创建server节点（示例）
 */
public class DistributeClient {
    private static final String connectString = "master:2181,slave1:2181,slave2:2181,slave3:2181";
    private static final int sessionTimeout = 2000;
    private ZooKeeper zk = null;

    //1.创建到zk的客户端连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new
                Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        //再次启动监听（为了多次执行监听）
                        try {
                            getServerList();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
    }

    //2.获取服务器列表信息
    public void getServerList() throws Exception {
        //2-1.选择监听路径进行监听
        String parentNode = "/servers";
        List<String> children = zk.getChildren(parentNode, true);
        //2-2.存储服务器信息列表
        ArrayList<String> servers = new ArrayList<>();
        //2-3.遍历所有节点，获取节点中的主机名称信息
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        //2-4.打印服务器列表信息
        System.out.println(servers);
    }

    //3.业务功能
    public void business() throws Exception {
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        //1.获取zk连接
        DistributeClient client = new DistributeClient();
        client.getConnect();
        //2.获取 servers 的子节点信息，从中获取服务器信息列表
        client.getServerList();
        //3.业务进程启动
        client.business();
    }
}
