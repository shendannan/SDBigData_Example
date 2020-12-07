package core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Persist3 {
    /*
        持久化作用1：代码重用，
        持久化作用2：数据执行较长，数据比较重要的场合也可以持久化提高安全性
     */
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
        //cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
        //mapRDD.cache()

        // 持久化操作必须在 action算子执行时 完成的，因为只有行动算子才读取了数据。
        mapRDD.persist(StorageLevel.DISK_ONLY)

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        println("**************************************")
        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)

        sc.stop()
    }
}
