package core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Dep2 {

    /**
     * 打印依赖关系
     * @param args null
     */
    def main(args: Array[String]): Unit = {

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("Dep")
        val sc = new SparkContext(sparConf)
        //OneToOneDependency:窄依赖 数据分区不变
        //ShuffleDependency：宽依赖 数据分区打乱
        val lines: RDD[String] = sc.textFile("spark-demo/data/1.txt")
        println(lines.dependencies)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("*************************")
        val wordToOne: RDD[(String, Int)] = words.map(word=>(word,1))
        println(wordToOne.dependencies)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.dependencies)
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        sc.stop()

    }
}
