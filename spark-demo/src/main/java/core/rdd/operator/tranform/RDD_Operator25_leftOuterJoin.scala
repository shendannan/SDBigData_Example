package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator25_leftOuterJoin {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 leftOuterJoin
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("b", 2) //, ("c", 3)
        ))

        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 4), ("b", 5), ("c", 6)
        ))
        val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
        val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)

        leftJoinRDD.collect().foreach(println)
        rightJoinRDD.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
