package hu.sztaki.mbalassi.flink.ials.tester.als.test.ials

object CompareFlinkSpark {

  def main(args: Array[String]): Unit = {

    val train = Petshop.data
    val test = Petshop.test
    val lambda = Recommendation.lambda
    val numIter = 2000
    val rank = Recommendation.numFactors
    val alpha = Recommendation.alpha

    val sparkPred = SparkALSTest.runALSTest(train, test, rank, lambda, numIter, alpha)
    val flinkPred = FlinkALSTest.runALSTest(train, test, rank, lambda, numIter, alpha)

    sparkPred.foreach(println)
    flinkPred.foreach(println)
  }

}
