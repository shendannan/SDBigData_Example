package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator18_reduceByKey {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 reduceByKey
        //可以将数据按照相同的 Key 对 Value 进行聚合
        // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
        // 【1，2，3】=>【3，3】=>【6】
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("b", 4)
        ))

        // reduceByKey中如果key的数据只有一个，是不会参与运算的。
        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
            println(s"x = $x, y = $y")
            x + y
        })

        reduceRDD.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
