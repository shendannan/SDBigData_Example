package core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_File {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        //[*]表示当前系统最大可用核数，缺省则为单核
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.创建RDD
        //从文件中创建RDD，将文件中的数据作为处理的数据源
        //path路径默认以当前环境的根路径为基准，可以写绝对路径或相对路径，可含有通配符
        val rdd1: RDD[String] = sc.textFile("spark-demo/data/1.txt")
        val rdd2: RDD[String] = sc.textFile("spark-demo/data/1*.txt")
        //path还可以是分布式文件系统路径
        val rdd3: RDD[String] = sc.textFile("hdfs://master:9000/sdn/1.txt")
        //path也可以指定目录名，将读取目录下所有文件
        //textFile：以行为单位读取，读取的都是字符串
        //wholeTextFiles：以文件为单位读取，结果为元组，第一项为文件路径，后续为文件内容
        val rdd4: RDD[String] = sc.textFile("spark-demo/data")
        val rdd5: RDD[(String, String)] = sc.wholeTextFiles("spark-demo/data")

        rdd1.collect().foreach(println)

        //3.关闭环境
        sc.stop()
    }
}
