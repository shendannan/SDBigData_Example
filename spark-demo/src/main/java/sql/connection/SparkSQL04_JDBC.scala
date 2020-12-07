package sql.connection

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL04_JDBC {

    def main(args: Array[String]): Unit = {

        //1. 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        //2. 读取MySQL数据
        val df: DataFrame = spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://linux1:3306/spark-sql") //本机地址：默认端口/数据库名
                .option("driver", "com.mysql.jdbc.Driver")//驱动程序
                .option("user", "root")
                .option("password", "123123")
                .option("dbtable", "user")//表名
                .load()
        //df.show

        //3. 保存数据
        df.write.format("jdbc")
                .option("url", "jdbc:mysql://linux1:3306/spark-sql")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "123123")
                .option("dbtable", "user1")
                .mode(SaveMode.Append)
                .save()

        //4. 关闭环境
        spark.close()
    }
}
