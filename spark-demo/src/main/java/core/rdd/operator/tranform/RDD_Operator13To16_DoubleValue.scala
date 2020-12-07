package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator13To16_DoubleValue {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 双Value类型
        // intersection,union,subtract
        // 交集，并集和差集要求两个数据源数据类型保持一致
        // zip 拉链操作两个数据源的类型可以不一致，将相同位置的数据拉起来
        // zip要求分区数量保持一致，分区元素数量也要保持一致
        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
        // 交集 : 【3，4】
        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        println("intersection:" + rdd3.collect().mkString(","))

        // 并集 : 【1，2，3，4，3，4，5，6】
        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println("union:" + rdd4.collect().mkString(","))

        // 差集 : 【1，2】
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        println("subtract:" + rdd5.collect().mkString(","))

        // 拉链 : 【1-3，2-4，3-5，4-6】
        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println("zip:" + rdd6.collect().mkString(","))

        val rdd7: RDD[String] = sc.makeRDD(List("a", "b", "c", "d"))
        val rdd8: RDD[(Int, String)] = rdd1.zip(rdd7)
        println("zip:" + rdd8.collect().mkString(","))
        //3.关闭环境
        sc.stop()
    }
}
