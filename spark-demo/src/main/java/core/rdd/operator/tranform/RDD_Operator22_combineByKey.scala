package core.rdd.operator.tranform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator22_combineByKey {
    def main(args: Array[String]): Unit = {
        //1.准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //2.转换算子 combineByKey
        //最通用的对 key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。类似于
        //aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3),
            ("b", 4), ("b", 5), ("a", 6)
        ), 2)
        // combineByKey : 方法需要三个参数
        // 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
        // 第二个参数表示：分区内的计算规则
        // 第三个参数表示：分区间的计算规则
        val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
            v => (v, 1),
            (t: (Int, Int), v) => {
                (t._1 + v, t._2 + 1)
            },
            (t1: (Int, Int), t2: (Int, Int)) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )

        val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
            case (num, cnt) => {
                num / cnt
            }
        }
        resultRDD.collect().foreach(println)

        /* reduceByKey、foldByKey、aggregateByKey、combineByKey 的区别？
            reduceByKey:
                 combineByKeyWithClassTag[V](
                     (v: V) => v, // 第一个值不会参与计算
                     func, // 分区内计算规则
                     func, // 分区间计算规则
                     )

            aggregateByKey :
                combineByKeyWithClassTag[U](
                    (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
                    cleanedSeqOp, // 分区内计算规则
                    combOp,       // 分区间计算规则
                    )

            foldByKey:
                combineByKeyWithClassTag[V](
                    (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
                    cleanedFunc,  // 分区内计算规则
                    cleanedFunc,  // 分区间计算规则
                    )

            combineByKey :
                combineByKeyWithClassTag(
                    createCombiner,  // 相同key的第一条数据进行的处理函数
                    mergeValue,      // 表示分区内数据的处理函数
                    mergeCombiners,  // 表示分区间数据的处理函数
                    )
         */
        //    rdd.reduceByKey(_+_) // wordcount
        //    rdd.aggregateByKey(0)(_+_, _+_) // wordcount
        //    rdd.foldByKey(0)(_+_) // wordcount
        //    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y) // wordcount

        //3.关闭环境
        sc.stop()
    }
}
