package core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Acc1 {

    def main(args: Array[String]): Unit = {

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        //reduce: 分区内计算，分区间计算，逻辑很复杂
        //val i: Int = rdd.reduce(_+_)
        //println(i)

        //几个分区并行修改sum，executor的sum的值不确定，但driver的sum没有变化
        var sum = 0
        rdd.foreach(
            num => {
                sum += num
            }
        )
        println("sum = " + sum)

        sc.stop()

    }
}
