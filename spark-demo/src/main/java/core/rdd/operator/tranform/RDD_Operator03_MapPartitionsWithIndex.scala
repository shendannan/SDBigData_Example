package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator03_MapPartitionsWithIndex {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 mapPartitionsWithIndex
        // 与mapPartitions功能相同，在处理时同时可以获取当前分区索引。
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val mpRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if (index == 1) {
                    iter.map(_ * 2)
                } else {
                    Nil.iterator
                }
            }
        )
        mpRDD.collect().foreach(println)

        /*
        //案例：获取每个数据的分区号
        val mpRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
          (index,iter) => {iter.map((index,_))}
        )
        mpRDD.collect().foreach(println)
        */

        //3.关闭环境
        sc.stop()
    }
}
