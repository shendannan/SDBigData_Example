package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator01_Map {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 map
        // 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val mapRDD: RDD[Int] = rdd.map(_ * 2)
        val mapRDD2: RDD[String] = mapRDD.map("~" + _)
        mapRDD2.collect().foreach(println)

        /*
        //案例：从服务器日志数据 apache.log 中获取用户请求 URL 资源路径
        val rdd: RDD[String] = sc.textFile("spark-demo/data/apache.log")
        val mapRDD: RDD[String] = rdd.map(
          line => {
            val datas = line.split(" ")
            datas(6)//找到URL的位置
          }
        )
        mapRDD.collect().foreach(println)
        */

        //3.关闭环境
        sc.stop()
    }
}
