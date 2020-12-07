package sql.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL02_UDF {

    def main(args: Array[String]): Unit = {

        //1. 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val df: DataFrame = spark.read.json("spark-demo/data/user.json")
        df.createOrReplaceTempView("user")

        //2. UDF:用户自定义函数，下面实现在查询name前加上前缀
        spark.udf.register("prefixName", (name:String) => {
            "Name: " + name
        })

        spark.sql("select age, prefixName(username) from user").show

        // TODO 关闭环境
        spark.close()
    }
}
