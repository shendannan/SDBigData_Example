package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator24_join {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 join
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("c", 3), ("d", 7)
        ))

        val rdd2: RDD[(String, String)] = sc.makeRDD(List(
            ("a", "5"), ("c", "6"), ("a", "4")
        ))

        // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
        //        如果两个数据源中key没有匹配上，那么数据不会出现在结果中
        //        如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。
        val joinRDD: RDD[(String, (Int, String))] = rdd1.join(rdd2)

        joinRDD.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
