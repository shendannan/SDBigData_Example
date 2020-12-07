package core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_File_Parallelism {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        //[*]表示当前系统最大可用核数，缺省则为单核
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.创建RDD
        //textFile的第二个参数用于设置最小分区，缺省为defaultMinPartitions
        //注意，设置的是最小分区，实际分区数大于等于该值
        //defaultMinPartitions: Int = math.min(defaultParallelism, 2)
        //defaultParallelism即为makeRDD中的缺省值
        val rdd1: RDD[String] = sc.textFile("spark-demo/data/1.txt", 2)

        rdd1.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
