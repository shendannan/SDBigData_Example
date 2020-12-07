package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator10_saveAsFile {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.行动算子 countByValue countByKey
        //统计每种 value 或 key 的个数
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1),("a", 2),("a", 3)
        ))
        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        // saveAsSequenceFile方法要求数据的格式必须为K-V类型
        rdd.saveAsSequenceFile("output2")
        //3.关闭环境
        sc.stop()
    }
}
