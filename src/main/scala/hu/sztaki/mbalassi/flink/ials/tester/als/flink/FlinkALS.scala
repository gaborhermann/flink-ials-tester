package hu.sztaki.mbalassi.flink.ials.tester.als.flink

import hu.sztaki.mbalassi.flink.ials.tester.als.correlation.Spearman
import hu.sztaki.mbalassi.flink.ials.tester.utils.Utils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector

object FlinkALS {

  case class ALSParams(
                        iterations: Int,
                        blocks: Option[Int],
                        numFactors: Int,
                        lambda: Double,
                        implicitPrefs: Boolean,
                        alpha: Double
                      )

  def main(args: Array[String]) {

    val propFileOption = Utils.parameterCheck(args)

    propFileOption.map(propFile => {
      // parse parameters
      val parsedArgs = ParameterTool.fromPropertiesFile(propFile)
      val inputFile = parsedArgs.getRequired("ALSInput")
      val outputFile = parsedArgs.getRequired("ALSOutput")

      val iterations = parsedArgs.getRequired("ALSIterations").toInt
      val numFactors = parsedArgs.getRequired("ALSNumFactors").toInt
      val lambda = parsedArgs.getRequired("ALSLambda").toDouble
      val blocks = parsedArgs.getInt("ALSBlocks", -1) match {
        case -1 => None
        case x => Some(x)
      }
      val implicitPrefs = parsedArgs.getRequired("ALSImplicitPrefs").equals("true")
      val alpha = parsedArgs.getRequired("ALSAlpha").toInt

      val alsParams = ALSParams(iterations, blocks, numFactors, lambda, implicitPrefs, alpha)

      // initialize Flink environment
      val env = ExecutionEnvironment.getExecutionEnvironment

      // Read and parse the input data: (timestamp, user, store == artist, 1)
      val input = env.readCsvFile[(Int, Int, Double)](inputFile, fieldDelimiter = ",")
      val test = notRatedUserItemPairs(input)

      val rankings = trainAndGetRankings(input, test, alsParams)

      rankings.writeAsCsv(outputFile, fieldDelimiter = ",")

      env.execute()
    }).getOrElse {
      println("\n\tPlease provide a properties file!")
    }
  }

  def allUserItemPairs(data: DataSet[(Int, Int, Double)]): DataSet[(Int, Int)] = {
    val users = data.map(_._1).distinct()
    val items = data.map(_._2).distinct()

    users cross items
  }

  def notRatedUserItemPairs(data: DataSet[(Int, Int, Double)]): DataSet[(Int, Int)] = {
    val rated = data.map(x => (x._1, x._2)).distinct()
    val all = allUserItemPairs(data)

    minus(all, rated)
  }

  // todo make generic
  type IntPair = (Int,Int)

  /**
    * Difference of two [[DataSet]]s.
    * Note, that only works on [[DataSet]]s containing distinct elements.
    * @param a
    * @param b
    * @return
    */
  def minus(a: DataSet[IntPair], b: DataSet[IntPair]): DataSet[IntPair] = {
    a.fullOuterJoin(b)
      .where(x => x)
      .equalTo(x => x)
      .apply(
        (x: IntPair, y: IntPair, out: Collector[IntPair]) => {
          if (x != null && y == null) {
            out.collect(x)
          }
        }
      )
  }

  def trainAndGetRankings(
                           train: DataSet[(Int, Int, Double)],
                           test: DataSet[(Int, Int)],
                           als: ALSParams
                         ): DataSet[(Int, Int, Double, Int)] = {
    val model = ALS()
      .setNumFactors(als.numFactors)
      .setIterations(als.iterations)
      .setLambda(als.lambda)
      .setImplicit(als.implicitPrefs)
      .setAlpha(als.alpha)

    for { b <- als.blocks } yield {
      model.setBlocks(b)
    }

    model.fit(train)

    val predictions = model.predict(test)

    Spearman.ranks(predictions)
  }

}
