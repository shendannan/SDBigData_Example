package sql.connection

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL05_Hive {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")
        //1. 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        //2. 使用SparkSQL连接外置的Hive
        //2-1. 拷贝Hive-size.xml文件到classpath下
        //2-2. 启用Hive的支持
        //2-3. 增加对应的依赖关系（包含MySQL的驱动maven包）
        spark.sql("show tables").show

        //  注意：在开发工具中创建数据库默认是在本地仓库，通过参数修改数据库仓库的地址:
        //  config("spark.sql.warehouse.dir", "hdfs://linux1:8020/user/hive/warehouse")

        //3. 关闭环境
        spark.close()
    }
}
