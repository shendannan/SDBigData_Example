package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator02_MapPartitions {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 mapPartitions
        // 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，包括过滤数据。
        // 优点：速度快，效果类似批处理，有几个分区执行几次，而不像 map 一个一个处理
        // 缺点：会将整个分区的数据加载到内存，存在对象引用使得内存不会被释放，在内存较小，数据量较大的场合
        // 下，易出现内存溢出
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        val mpRDD: RDD[Int] = rdd.mapPartitions(
            iter => {
                println(">>>>>>>>>>>>")
                iter.map(_ * 2)
            }
        )
        mpRDD.collect().foreach(println)

        /*
        //案例：获取每个数据分区的最大值
        val mpRDD: RDD[Int] = rdd.mapPartitions(
          iter => {
            List(iter.max).iterator
          }
        )
        mpRDD.collect().foreach(println)
        */

        //3.关闭环境
        sc.stop()
    }
}
