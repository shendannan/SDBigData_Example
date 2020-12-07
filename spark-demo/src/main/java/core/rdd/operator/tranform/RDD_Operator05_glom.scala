package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator05_glom {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 glom
        // 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
        // List => Int  Int => Array
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        val glomRDD: RDD[Array[Int]] = rdd.glom()
        glomRDD.collect().foreach(data => println(data.mkString(",")))

        /*
        //案例：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        // 【1，2】，【3，4】=>【2】，【4】=>【6】
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
        val glomRDD: RDD[Array[Int]] = rdd.glom()
        val maxRDD: RDD[Int] = glomRDD.map(array => {array.max})
        println(maxRDD.collect().sum)
        */

        //3.关闭环境
        sc.stop()
    }
}
