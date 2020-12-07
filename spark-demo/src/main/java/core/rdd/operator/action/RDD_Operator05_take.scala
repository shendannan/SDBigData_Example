package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator05_take {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.行动算子 take
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        //获取前N个数据
        val ints: Array[Int] = rdd.take(3)
        println(ints.mkString(","))

        //3.关闭环境
        sc.stop()
    }
}
