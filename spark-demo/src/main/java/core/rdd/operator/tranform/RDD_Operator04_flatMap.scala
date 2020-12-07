package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator04_flatMap {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 flatMap
        // 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
        val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
        val flatRDD: RDD[Int] = rdd.flatMap(list => list)
        flatRDD.collect().foreach(println)

        /*
        //案例1：分词
        val rdd1: RDD[String] = sc.makeRDD(List("Hello scala","Hello world"))
        val flatRDD1: RDD[String] = rdd1.flatMap(s => s.split(" "))
        flatRDD1.collect().foreach(println)
        //案例2：将 List(List(1,2),3,List(4,5))进行扁平化操作
        val rdd2: RDD[Any] = sc.makeRDD(List(List(1, 2),3,List(4,5)))
        //采用模式匹配
        val flatRDD2: RDD[Any] = rdd2.flatMap {
          case list: List[_] => list
          case data => List(data)
        }
        flatRDD2.collect().foreach(println)
        */

        //3.关闭环境
        sc.stop()
    }
}
