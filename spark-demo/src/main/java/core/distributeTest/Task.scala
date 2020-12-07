package core.distributeTest

class Task extends Serializable {
    var datas: List[Int] = _

    //匿名函数
    var logic = (num: Int) => num

    //计算
    def compute(): List[Int] = {
        datas.map(logic)
    }

}
