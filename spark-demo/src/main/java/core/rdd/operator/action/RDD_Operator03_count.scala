package core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator03_count {

    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.行动算子 count
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        //计算数据源中数据的个数
        val cnt: Long = rdd.count()
        println(cnt)
        //3.关闭环境
        sc.stop()
    }
}
