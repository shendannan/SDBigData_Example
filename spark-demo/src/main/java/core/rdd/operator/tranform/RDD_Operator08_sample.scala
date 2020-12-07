package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator08_sample {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 sample
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        // sample算子需要传递三个参数
        // 1. 第一个参数表示，抽取数据后是否将数据返回 true（放回），false（丢弃）
        // 2. 第二个参数表示，
        //    如果抽取不放回的场合：数据源中每条数据被抽取的概率，基准值的概念
        //    如果抽取放回的场合：表示数据源中的每条数据被抽取的可能次数
        // 3. 第三个参数表示，抽取数据时随机算法的种子，默认为当前系统时间
        // 可在数据倾斜的时候使用
        println(rdd.sample(
            withReplacement = false,
            0.4
            //1
        ).collect().mkString(","))

        println(rdd.sample(
            withReplacement = true,
            2
            //1
        ).collect().mkString(","))

        //3.关闭环境
        sc.stop()
    }
}
