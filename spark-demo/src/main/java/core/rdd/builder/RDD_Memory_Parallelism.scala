package core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_Memory_Parallelism {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        //[*]表示当前系统最大可用核数，缺省则为单核
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        //    sparkConf.set("spark.default.parallelism","5")
        val sc = new SparkContext(sparkConf)

        //2.创建RDD
        //RDD的并行度 & 分区
        //makeRDD方法的第二个参数代表分区的数量，缺省值为defaultParallelism默认并行度
        //源码：scheduler.conf.getInt("spark.default.parallelism",totalCores)
        //spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
        //如果获取不到，则使用totalCores属性，这个属性取值为当前运行环境下的最大可用核数
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        //将处理的数据保存成分区文件
        rdd.saveAsTextFile("spark-demo/output")

        //3.关闭环境
        sc.stop()
    }
}
