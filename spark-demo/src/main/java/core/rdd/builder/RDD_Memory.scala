package core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memory {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        //[*]表示当前系统最大可用核数，缺省则为单核
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.创建RDD
        //从内存中创建RDD，将内存中集合的数据作为处理的数据源
        val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
        //parallelize: 并行，下面两行都可以，底层实现相同
        //    val rdd: RDD[Int] = sc.parallelize(seq)
        val rdd: RDD[Int] = sc.makeRDD(seq)

        rdd.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
