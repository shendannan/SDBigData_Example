package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator21_foldByKey {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 foldByKey
        //将数据根据相同的规则进行分区内计算和分区间计算
        // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3),
            ("b", 4), ("b", 5), ("a", 6)
        ), 2)

        //rdd.aggregateByKey(0)(_+_, _+_).collect.foreach(println)
        rdd.foldByKey(0)(_ + _).collect.foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
