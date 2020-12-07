package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator20_aggregateByKey {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 aggregateByKey
        //将数据根据不同的规则进行分区内计算和分区间计算
        // 例如取分区内最大值求和  (a,【1,2】), (a, 【3，4】)
        // (a, 2), (a, 4)
        // (a, 6)
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("a", 4)
        ), 2)

        // aggregateByKey存在函数柯里化，有两个参数列表
        // 第一个参数列表,需要传递一个参数，表示为初始值
        //       主要用于当碰见第一个key的时候，和value进行分区内计算
        // 第二个参数列表需要传递2个参数
        //      第一个参数表示分区内计算规则
        //      第二个参数表示分区间计算规则
        rdd.aggregateByKey(0)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        ).collect.foreach(println)

        // 注意：aggregateByKey最终的返回数据结果应该和初始值（第一个参数列表）的类型保持一致
        val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
        aggRDD.collect.foreach(println)

        /*
        //案例：获取相同key的数据的平均值 => (a, 3),(b, 4)
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
          ("a", 1), ("a", 2), ("b", 3),
          ("b", 4), ("b", 5), ("a", 6)
        ),2)

        val newRDD : RDD[(String, (Int, Int))] = rdd.aggregateByKey((0,0))(
          (t, v) => {
            (t._1 + v, t._2 + 1)
          },
          (t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2)
          }
        )

        val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
          case (num, cnt) =>
            num / cnt
        }
        resultRDD.collect().foreach(println)
        */

        //3.关闭环境
        sc.stop()
    }
}
