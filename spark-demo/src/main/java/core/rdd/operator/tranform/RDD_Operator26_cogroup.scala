package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator26_cogroup {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 cogroup connect+group
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("b", 2) //, ("c", 3)
        ))

        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 4), ("b", 5), ("c", 6), ("c", 7)
        ))

        // cogroup : connect + group (分组，连接)
        val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

        cgRDD.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
