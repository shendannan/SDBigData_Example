package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator10_coalesce {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 coalesce
        // 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
        // 当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，
        // 收缩合并分区，减少分区的个数，减小任务调度成本
        // coalesce方法默认情况下不会将分区的数据打乱重新组合
        // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
        // 如果想要让数据均衡，可以进行shuffle处理
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
        val newRDD: RDD[Int] = rdd.coalesce(2, shuffle = true)

        newRDD.saveAsTextFile("output")

        //3.关闭环境
        sc.stop()
    }
}
