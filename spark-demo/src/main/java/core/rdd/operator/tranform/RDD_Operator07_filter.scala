package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator07_filter {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 filter
        // 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
        // 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出
        // 现数据倾斜。
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)
        filterRDD.collect().foreach(println)

        /*
        //案例：从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
        val rdd: RDD[String] = sc.textFile("spark-demo/data/apache.log")

        rdd.filter(
          line => {
            val datas: Array[String] = line.split(" ")
            val time: String = datas(3)
            time.startsWith("17/05/2015")
          }
        ).collect().foreach(println)
       */

        //3.关闭环境
        sc.stop()
    }
}
