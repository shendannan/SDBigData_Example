package sql.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL03_UDAF1 {

    def main(args: Array[String]): Unit = {

        //1. 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val df: DataFrame = spark.read.json("spark-demo/data/user.json")
        df.createOrReplaceTempView("user")
        //functions.udaf 将强类型操作转换为弱类型的sql  注：2.4.4等早期版本没有此方法，详见SparkSQL03_UDAF2.scala
        spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

        spark.sql("select ageAvg(age) from user").show

        //2. 关闭环境
        spark.close()
    }
    /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
         IN : 输入的数据类型 Long
         BUF : 缓冲区的数据类型 Buff
         OUT : 输出的数据类型 Long
     2. 重写方法(6)
     */
    case class Buff( var total:Long, var count:Long )
    class MyAvgUDAF extends Aggregator[Long, Buff, Long]{
        // z & zero : 初始值或零值
        // 缓冲区的初始化
        override def zero: Buff = {
            Buff(0L,0L)
        }

        // 根据输入的数据更新缓冲区的数据
        override def reduce(buff: Buff, in: Long): Buff = {
            buff.total = buff.total + in
            buff.count = buff.count + 1
            buff
        }

        // 合并缓冲区
        override def merge(buff1: Buff, buff2: Buff): Buff = {
            buff1.total = buff1.total + buff2.total
            buff1.count = buff1.count + buff2.count
            buff1
        }

        //计算结果
        override def finish(buff: Buff): Long = {
            buff.total / buff.count
        }

        // 缓冲区的编码操作（固定写法）
        override def bufferEncoder: Encoder[Buff] = Encoders.product

        // 输出的编码操作（固定写法）
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
