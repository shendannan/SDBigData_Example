package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator01_reduce {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.行动算子 reduce
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // 聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
        val i: Int = rdd.reduce(_+_)
        println(i)

        //3.关闭环境
        sc.stop()
    }
}
