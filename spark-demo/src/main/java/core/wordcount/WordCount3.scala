package core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("spark-demo/data")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

        //Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
        //reduceByKey: 相同key的数据可以对value进行聚合
        //wordToOne.reduceByKey((x,y)=>{x + y})
        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)

        sc.stop()
    }
}
