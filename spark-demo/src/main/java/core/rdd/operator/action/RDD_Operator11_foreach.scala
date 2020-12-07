package core.rdd.operator.action

import core.rdd.operator.action.Spark07_RDD_Operator_Action.User
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator11_foreach {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)
        //2.行动算子 countByValue countByKey
        //统计每种 value 或 key 的个数
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        // foreach 其实是Driver端内存集合的循环遍历方法
        rdd.collect().foreach(println)
        println("******************")

        // foreach 其实是Executor端内存数据打印
        rdd.foreach(println)

        // 算子 ： Operator（操作）
        //        RDD的方法和Scala集合对象的方法不一样
        //        集合对象的方法都是在同一个节点的内存中完成的。
        //        RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
        //        为了区分不同的处理效果，所以将RDD的方法称之为算子。
        //        RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。
        /*
        //案例：分布式 将外部方法用算子打印
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        val user = new User()

        rdd.foreach(
            num => {
                println("age = " + (user.age + num))
            }
        )
        */

        //3.关闭环境
        sc.stop()
    }

    class User extends Serializable {
    // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
    //case class User() {
        var age : Int = 30
    }
}
