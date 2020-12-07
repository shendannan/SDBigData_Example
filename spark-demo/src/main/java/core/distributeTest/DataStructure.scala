package core.distributeTest

/**
 * 这里类比于RDD
 */
class DataStructure extends Serializable {
    //注意：要混入Serializable才能序列化传输对象
    val datas = List(1, 2, 3, 4)
    //匿名函数
    val logic = (num: Int) => num * 2

}
