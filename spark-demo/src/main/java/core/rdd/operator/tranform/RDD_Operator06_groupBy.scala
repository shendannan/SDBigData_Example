package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator06_groupBy {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 groupBy
        // 将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
        // 相同的key值的数据会放置在一个组中，分区默认不变，分区和分组没有必然关系
        // 一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        def groupFunction(num: Int): Int = {
            num % 2
        }

        val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
        groupRDD.collect().foreach(println)

        /*
        //案例1：相同首字母分组
        val rdd : RDD[String] = sc.textFile("spark-demo/data")
        val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
        groupRDD.collect().foreach(println)
        //案例2：从服务器日志数据 apache.log 中获取每个时间段访问量
        val rdd: RDD[String] = sc.textFile("spark-demo/data/apache.log")

        val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
          line => {
            val datas: Array[String] = line.split(" ")
            val time: String = datas(3)
            val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val date: Date = sdf.parse(time)
            val sdf1 = new SimpleDateFormat("HH")
            val hour: String = sdf1.format(date)
            (hour, 1)
          }
        ).groupBy(_._1)
        timeRDD.map {
          case (hour, iter) =>
            (hour, iter.size)
        }.collect.foreach(println)
        */

        //3.关闭环境
        sc.stop()
    }
}
