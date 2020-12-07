package core.rdd.operator.tranform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Case {
    /**
     * agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
     * 统计出每一个省份每个广告被点击数量排行的 Top3
     *
     * @param args null
     */
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.获取原始数据：时间戳，省份，城市，用户，广告
        val dataRDD: RDD[String] = sc.textFile("spark-demo/data/agent.log")

        //3. 将原始数据进行结构的转换，删除多余信息，方便统计
        //   注意将省份和广告放在一起作为key，用于后续分组
        //   时间戳，省份，城市，用户，广告 =>((省份，广告), 1)
        val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
            line => {
                val datas: Array[String] = line.split(" ")
                ((datas(1), datas(4)), 1)
            }
        )

        //4. 将转换结构后的数据，进行分组聚合
        //   ((省份，广告), 1) => ((省份，广告), sum)
        val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

        //5. 将聚合的结果进行结构的转换
        //   ((省份，广告), sum) => (省份, (广告, sum))
        val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
            case ((prv, ad), sum) =>
                (prv, (ad, sum))
        }

        //6. 将转换结构后的数据根据省份进行分组
        //   (省份, 【(广告A, sumA)，(广告B, sumB)】)
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

        //7. 将分组后的数据组内排序（降序），取前3名
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        //8. 采集数据打印在控制台
        resultRDD.collect().foreach(println)

        //9.关闭环境
        sc.stop()
    }
}
