package core.framework.service

import core.framework.common.TService
import core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
  * 服务层
  */
class WordCountService extends TService {

    private val wordCountDao = new WordCountDao()

    // 数据分析
    def dataAnalysis(): Array[(String, Int)] = {

        val lines: RDD[String] = wordCountDao.readFile("spark-demo/data/1.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = words.map(word=>(word,1))
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        val array: Array[(String, Int)] = wordToSum.collect()
        array
    }
}
