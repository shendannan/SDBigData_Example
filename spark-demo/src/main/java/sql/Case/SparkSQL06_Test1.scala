package sql.Case

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL06_Test1 {
    //TODO 需求简介
    //  本次案例Spark-sql操作中所有的数据均来自 Hive，首先在 Hive 中创建表，并导入数据。
    //  一共有3张表： 1张用户行为表，1张城市表，1张产品表
    //STEP1 : 数据准备
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        spark.sql("use mydatabase")

        // 准备数据
        spark.sql(
            """
              |CREATE TABLE `user_visit_action`(
              |  `date` string,
              |  `user_id` bigint,
              |  `session_id` string,
              |  `page_id` bigint,
              |  `action_time` string,
              |  `search_keyword` string,
              |  `click_category_id` bigint,
              |  `click_product_id` bigint,
              |  `order_category_ids` string,
              |  `order_product_ids` string,
              |  `pay_category_ids` string,
              |  `pay_product_ids` string,
              |  `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'data/SparkSQL/user_visit_action.txt' into table mydatabase.user_visit_action
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `product_info`(
              |  `product_id` bigint,
              |  `product_name` string,
              |  `extend_info` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'data/SparkSQL/product_info.txt' into table mydatabase.product_info
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `city_info`(
              |  `city_id` bigint,
              |  `city_name` string,
              |  `area` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'data/SparkSQL/city_info.txt' into table mydatabase.city_info
            """.stripMargin)

        spark.sql("""select * from city_info""").show


        spark.close()
    }
}
