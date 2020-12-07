package core.Case

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategoryTop10Analysis_03 {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        //  方案三：一次性统计每个品类点击的次数，下单的次数和支付的次数、
        //  （品类，（点击数，下单数，支付数））
        val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        // HotCategoryTop10Analysis_02的问题 : 存在大量的shuffle操作（reduceByKey）
        // 注意，reduceByKey聚合算子，spark会提供优化，自动缓存，但我们的方法针对的数据源不一样，所以必须存在shuffle

        // 1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile("spark-demo/data/user_visit_action.txt")

        // 2. 将数据转换结构
        //    点击的场合 : ( 品类ID，( 1, 0, 0 ) )
        //    下单的场合 : ( 品类ID，( 0, 1, 0 ) )
        //    支付的场合 : ( 品类ID，( 0, 0, 1 ) )
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids: Array[String] = datas(8).split(",")
                    ids.map(id => (id, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids: Array[String] = datas(10).split(",")
                    ids.map(id => (id, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        )

        // 3. 将相同的品类ID的数据进行分组聚合
        //    ( 品类ID，( 点击数量, 下单数量, 支付数量 ) )
        val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
            (t1, t2) => {
                ( t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )

        // 4. 将统计结果根据数量进行降序处理，取前10名
        val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, ascending = false).take(10)

        // 5. 将结果采集到控制台打印出来
        resultRDD.foreach(println)

        sc.stop()
    }
}
