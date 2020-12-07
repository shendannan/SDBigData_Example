package core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Acc3 {

    def main(args: Array[String]): Unit = {

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // 获取系统累加器
        // Spark默认就提供了简单数据聚合的累加器
        val sumAcc: LongAccumulator = sc.longAccumulator("sum")

        //sc.doubleAccumulator
        //sc.collectionAccumulator

        val mapRDD: RDD[Int] = rdd.map(
            num => {
                // 使用累加器
                sumAcc.add(num)
                num
            }
        )

        // 获取累加器的值
        // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
        // 多加：转换算子中调用累加器，每次调用行动算子都会执行一次
        // 一般情况下，累加器会放置在行动算子进行操作
        mapRDD.collect()
        mapRDD.collect()
        println(sumAcc.value)

        sc.stop()

    }
}
