package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDD_Operator17_partitionBy {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 partitionBy
        //partitionBy根据指定的分区规则对数据进行重分区
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
        // RDD => PairRDDFunctions  隐式转换（二次编译）
        val newRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))
        newRDD.saveAsTextFile("output")

        //3.关闭环境
        sc.stop()
    }
}
