package core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Acc2 {

    def main(args: Array[String]): Unit = {

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // 获取系统累加器
        // Spark默认就提供了简单数据聚合的累加器
        val sumAcc: LongAccumulator = sc.longAccumulator("sum") //名字随便起，监控使用

        //sc.doubleAccumulator
        //sc.collectionAccumulator（后台使用util.List）

        rdd.foreach(
            num => {
                // 使用累加器
                sumAcc.add(num)
            }
        )
        // 获取累加器的值
        println(sumAcc.value)
        sc.stop()
    }
}
