package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator02_collect {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.行动算子 collect
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        //将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
        val ints: Array[Int] = rdd.collect()
        println(ints.mkString(","))

        //3.关闭环境
        sc.stop()
    }
}
