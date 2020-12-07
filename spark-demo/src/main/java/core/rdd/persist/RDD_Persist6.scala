package core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Persist6 {

    def main(args: Array[String]): Unit = {

        // cache & persist:
        //         会在血缘关系中添加新的依赖。一旦出现问题，可以重头读取数据
        // checkpoint :
        //         执行过程中，会切断血缘关系。重新建立新的血缘关系
        //         checkpoint等同于改变数据源

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        sc.setCheckpointDir("cp")

        val list = List("Hello Scala", "Hello Spark")

        val rdd: RDD[String] = sc.makeRDD(list)

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map(word=>{
            (word,1)
        })
        //mapRDD.cache()
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)
        println("**************************************")
        println(mapRDD.toDebugString)

        sc.stop()
    }
}
