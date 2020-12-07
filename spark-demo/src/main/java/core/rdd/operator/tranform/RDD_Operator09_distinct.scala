package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator09_distinct {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 distinct
        // 将数据集中重复的数据去重
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
        // 原理：map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
        // (1, null),(2, null),(3, null),(4, null),(1, null),(2, null),(3, null),(4, null)
        // (1, null)(1, null)
        // (null, null) => null
        // (1, null) => 1
        rdd.distinct().collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
