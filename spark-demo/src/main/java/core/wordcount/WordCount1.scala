package core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount1 {
    def main(args: Array[String]): Unit = {
        //Application
        //Spark框架
        //1.建立和Spark框架的连接
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        //2.执行业务操作
        //2-1.读取文件，获取一行一行数据
        //hello world
        val lines: RDD[String] = sc.textFile("spark-demo/data")

        //2-2.将一行数据进行拆分，形成一个一个单词（分词）
        //"hello world"=>hello,world,hello,world
        val words: RDD[String] = lines.flatMap(_.split(" "))

        //2-3.将数据根据单词进行分组，便于统计
        //(hello,hello,hello),(world,world)
        val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

        //2-4.对分组后数据进行转换
        //(hello,3),(world,2)
        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) => (word, list.size)
        }

        //2-5.将转换结果采集到控制台打印
        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)

        //3.关闭连接
        sc.stop()
    }
}
