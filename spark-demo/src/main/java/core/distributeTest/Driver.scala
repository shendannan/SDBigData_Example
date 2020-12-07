package core.distributeTest

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
    def main(args: Array[String]): Unit = {
        //连接服务器
        val client1 = new Socket("localhost", 9999)
        val client2 = new Socket("localhost", 8888)

        val task = new DataStructure()

        val out1: OutputStream = client1.getOutputStream
        val subTask1 = new Task()
        subTask1.logic = task.logic
        subTask1.datas = task.datas.take(task.datas.length / 2)

        val objOut1 = new ObjectOutputStream(out1)
        objOut1.writeObject(subTask1)
        objOut1.flush()
        objOut1.close()
        client1.close()

        val out2: OutputStream = client2.getOutputStream
        val subTask2 = new Task()
        subTask2.logic = task.logic
        subTask2.datas = task.datas.takeRight(task.datas.length / 2)

        val objOut2 = new ObjectOutputStream(out2)
        objOut2.writeObject(subTask2)
        objOut2.flush()
        objOut2.close()
        client2.close()

        println("客户端数据发送完毕")
    }
}
