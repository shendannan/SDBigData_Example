package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator12_sortBy {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 sortBy
        //该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理
        //的结果进行排序，默认为升序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一
        //致。中间存在 shuffle 的过程
        val rdd: RDD[Int] = sc.makeRDD(List(6, 2, 4, 5, 3, 1), 2)
        val newRDD: RDD[Int] = rdd.sortBy(num => num)
        newRDD.saveAsTextFile("output")

        /*
        //案例：按照key的大小进行排序
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
        // sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
        // sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
        val newRDD: RDD[(String, Int)] = rdd.sortBy(t=>t._1.toInt, ascending = false)
        newRDD.collect().foreach(println)
        */

        //3.关闭环境
        sc.stop()
    }
}
