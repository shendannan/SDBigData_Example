package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator09_countByKey {

    def main(args: Array[String]): Unit = {
      //1.准备环境
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
      val sc = new SparkContext(sparkConf)

      //2.行动算子 countByValue countByKey
      //统计每种 value 或 key 的个数
      val rdd1: RDD[Int] = sc.makeRDD(List(1,1,1,4),2)
      val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
        ("a", 1),("a", 2),("a", 3)
      ))
      val intToLong: collection.Map[Int, Long] = rdd1.countByValue()
      println(intToLong)
      val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
      println(stringToLong)
      //3.关闭环境
      sc.stop()
    }
}
