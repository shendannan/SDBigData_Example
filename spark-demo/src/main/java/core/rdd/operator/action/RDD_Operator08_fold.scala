package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator08_fold {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.行动算子 fold
        //aggregate的简化版，分区内和分区间执行相同操作
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        val result: Int = rdd.fold(10)(_+_)
        println(result)

        //3.关闭环境
        sc.stop()
    }
}
