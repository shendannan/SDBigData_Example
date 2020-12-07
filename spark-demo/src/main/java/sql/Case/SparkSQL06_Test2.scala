package sql.Case

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL06_Test2 {
    //TODO 需求简介
    //  这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每
    //  个商品在主要城市中的分布比例，超过两个城市用其他显示。
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        //1. 查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与Product_info表连接得到产品名称
        //2. 按照地区和商品id分组，统计出每个商品在每个地区的总点击次数
        //3. 每个地区内按照点击次数降序排列，只取前三名
        //4. 城市备注需要自定义 UDAF 函数(在Test3中实现）

        spark.sql("use mydatabase")

        spark.sql(
            """
              |select
              |    *
              |from (
              |    select
              |        *,
              |        rank() over( partition by area order by clickCnt desc ) as rank
              |    from (
              |        select
              |           area,
              |           product_name,
              |           count(*) as clickCnt
              |        from (
              |            select
              |               a.*,
              |               p.product_name,
              |               c.area,
              |               c.city_name
              |            from user_visit_action a
              |            join product_info p on a.click_product_id = p.product_id
              |            join city_info c on a.city_id = c.city_id
              |            where a.click_product_id > -1
              |        ) t1 group by area, product_name
              |    ) t2
              |) t3 where rank <= 3
            """.stripMargin).show

        spark.close()
    }
}
