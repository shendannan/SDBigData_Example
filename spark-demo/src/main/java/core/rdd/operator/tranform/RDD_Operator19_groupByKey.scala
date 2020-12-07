package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator19_groupByKey {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 groupByKey
        //可以将数据按照相同的 Key 对 Value 进行分组
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("b", 4)
        ))

        // groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
        //              元组中的第一个元素就是key，
        //              元组中的第二个元素就是相同key的value的集合
        val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        groupRDD.collect().foreach(println)
        //注意groupByKey和groupBy的区别：后者可以选择分组原则，组里包含每一项的key，而前者
        // 只根据key分组，组里只包含值。
        val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)


        //3.关闭环境
        sc.stop()
    }
}
