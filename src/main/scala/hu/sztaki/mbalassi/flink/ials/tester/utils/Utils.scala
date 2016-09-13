package hu.sztaki.mbalassi.flink.ials.tester.utils

object Utils {

  def parameterCheck(args: Array[String]): Unit = {
    def outputNoParamMessage(): Unit = {
      val noParamMsg = "\tUsage:\n\n\t./run <path to paramaters file>"
      println(noParamMsg)
    }

    if (args.length == 0 || !(new java.io.File(args(0)).exists)) {
      outputNoParamMessage()
    }
  }

}
