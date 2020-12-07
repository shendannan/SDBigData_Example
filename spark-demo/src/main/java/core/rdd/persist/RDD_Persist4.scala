package core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Persist4 {

    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        //注意：checkpoint需要落盘，需要指定检查点保存路径
        // 一般保存路径都是在分布式存储系统：HDFS，此处保存在本地
        sc.setCheckpointDir("cp")

        val list = List("Hello Scala", "Hello Spark")

        val rdd: RDD[String] = sc.makeRDD(list)

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map(word=>{
            println("@@@@@@@@@@@@")
            (word,1)
        })

        // 检查点路径保存的文件，当作业执行完毕后，不会被删除
        mapRDD.checkpoint()

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        println("**************************************")
        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)


        sc.stop()
    }
}
