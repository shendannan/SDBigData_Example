package sql.basic

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL01_Basic {
    //RDD DataFrame DataSet的创建和相互转化
    def main(args: Array[String]): Unit = {

        //1. 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        ///RDD=>DataFrame=>DataSet 转换需要引入隐式转换规则，否则无法转换
        //注意spark不是包名，是上下文环境对象名
        import spark.implicits._

        //2. DataFrame
        println("创建DataFrame")
        val df: DataFrame = spark.read.json("spark-demo/data/user.json")
        df.createOrReplaceTempView("user")

        // DataFrame => SQL 需要先创建视图
        println("DataFrame => SQL")
        df.createOrReplaceTempView("user") //视图只可查 不可改

        spark.sql("select * from user").show
        spark.sql("select username from user").show
        spark.sql("select avg(age) from user").show

        // DataFrame => DSL
        println("DataFrame => DSL")
        // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
        df.select("age", "username").show
        df.select($"age" + 1).show
        df.select('age + 1).show

        //3. DataSet (DataFrame其实是特定泛型的DataSet)
        println("创建DataSet")
        val seq = Seq(1,2,3,4)
        val ds: Dataset[Int] = seq.toDS()
        ds.show()

        // RDD <=> DataFrame
        val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
        val df1: DataFrame = rdd.toDF("id", "name", "age")
        val rowRDD: RDD[Row] = df1.rdd //返回的RDD类型为Row，里面提供的 getXXX 方法可以获取字段值，类似JDBC处理结果集，但是索引从0开始

        // DataFrame <=> DataSet
        val ds1: Dataset[User] = df1.as[User]
        val df2: DataFrame = ds.toDF()

        // RDD <=> DataSet
        val ds2: Dataset[User] = rdd.map {
            case (id, name, age) => User(id, name, age)
        }.toDS()
        val userRDD: RDD[User] = ds2.rdd

        // TODO 关闭环境
        spark.close()
    }

    case class User( id:Int, name:String, age:Int )
}
