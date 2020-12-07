package core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Persist2 {

    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)

        val list = List("Hello Scala", "Hello Spark")

        val rdd: RDD[String] = sc.makeRDD(list)

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map(word=>{
            println("@@@@@@@@@@@@")
            (word,1)
        })

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        println("**************************************")
        //如果一个RDD需要重复使用，需要从头再走一遍
        //对象可以重用，但数据没有重用
        //虽然代码看起来简洁，但和RDD_Persist1没有任何区别
        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)

        sc.stop()
    }
}
