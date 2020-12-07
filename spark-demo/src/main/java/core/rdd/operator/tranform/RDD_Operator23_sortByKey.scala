package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator23_sortByKey {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 sortByKey
        //在一个(K,V)的RDD上调用，K必须实现Ordered接口(特质)，返回一个按照key进行排序的
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("b", 2), ("c", 3), ("d", 4)
        ), 2)
        val sortRDD1: RDD[(String, Int)] = rdd.sortByKey(ascending = true)
        val sortRDD2: RDD[(String, Int)] = rdd.sortByKey(ascending = false)
        sortRDD1.collect().foreach(println)
        sortRDD2.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
