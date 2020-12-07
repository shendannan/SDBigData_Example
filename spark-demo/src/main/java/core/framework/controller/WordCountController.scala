package core.framework.controller

import core.framework.common.TController
import core.framework.service.WordCountService


/**
  * 控制层
  */
class WordCountController extends TController {

    private val wordCountService: WordCountService = new WordCountService()

    // 调度
    def dispatch(): Unit = {
        // TODO 执行业务操作
        val array: Array[(String, Int)] = wordCountService.dataAnalysis()
        array.foreach(println)
    }
}
