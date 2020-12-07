package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator11_repartition {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 repartition
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

        // coalesce算子可以扩大分区的，但是如果不进行shuffle操作，没有意义，不起作用。
        // 所以如果想要实现扩大分区的效果，需要使用shuffle操作
        // spark提供了一个简化的操作
        // 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
        // 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle
        //val newRDD: RDD[Int] = rdd.coalesce(3, true)
        val newRDD: RDD[Int] = rdd.repartition(3)

        newRDD.saveAsTextFile("output")

        //3.关闭环境
        sc.stop()
    }
}
