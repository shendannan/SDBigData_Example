package core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("spark-demo/data")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) =>
                list.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
        }

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)

        sc.stop()
    }
}
