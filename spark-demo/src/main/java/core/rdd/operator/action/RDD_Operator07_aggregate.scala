package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator07_aggregate {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.行动算子 aggregate
        //分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        //10 + 13 + 17 = 40
        // aggregateByKey : 初始值只会参与分区内计算
        // aggregate : 初始值会参与分区内计算,并且和参与分区间计算
        val result: Int = rdd.aggregate(10)(_+_, _+_)
        println(result)

        //3.关闭环境
        sc.stop()
    }
}
