package core.distributeTest

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
    def main(args: Array[String]): Unit = {
        //启动服务器，接收数据
        val server = new ServerSocket(9999)
        println("服务器[9999]启动，等待接收数据")
        //等待客户端连接
        val client: Socket = server.accept()
        val in: InputStream = client.getInputStream
        //    val i: Int = in.read()
        //    println("接收到客户端发送的数据：" + i)

        val objIn = new ObjectInputStream(in)
        val task: Task = objIn.readObject().asInstanceOf[Task]
        val ints: List[Int] = task.compute()

        println("计算节点[9999]计算的结果为：" + ints)
        objIn.close()

        client.close()
        server.close()
    }
}
