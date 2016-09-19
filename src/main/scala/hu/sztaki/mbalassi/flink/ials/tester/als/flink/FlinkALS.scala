package hu.sztaki.mbalassi.flink.ials.tester.als.flink

import hu.sztaki.mbalassi.flink.ials.tester.als.correlation.Spearman
import hu.sztaki.mbalassi.flink.ials.tester.utils.Utils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector

object FlinkALS {

  val smallRatings = Seq(
    (0, 3, 1.0),
    (0, 6, 2.0),
    (0, 9, 1.0),
    (1, 0, 1.0),
    (1, 2, 3.0),
    (1, 6, 1.0),
    (1, 7, 5.0),
    (1, 8, 1.0),
    (2, 1, 1.0),
    (2, 4, 3.0),
    (3, 1, 2.0),
    (3, 3, 4.0),
    (3, 5, 5.0),
    (4, 5, 1.0),
    (4, 8, 2.0),
    (4, 10, 2.0),
    (5, 2, 1.0)
  )

  case class ALSParams(
                        iterations: Int,
                        blocks: Option[Int],
                        numFactors: Int,
                        lambda: Double,
                        implicitPrefs: Boolean,
                        alpha: Double,
                        inputFile: String,
                        testInputFile: String,
                        outputFile: String,
                        topK: Option[Int]
                      )

  def main(args: Array[String]) {

    val propFileOption = Utils.parameterCheck(args)

    propFileOption.map(propFile => {
      // parse parameters
      val alsParams = parseALSParams(propFile)

      // initialize Flink environment
      val env = ExecutionEnvironment.getExecutionEnvironment

      env.setParallelism(1)

      // Read and parse the input data:
      // last fm schema: 'time','user','item','id','score','eval'
      val input = env.readCsvFile[(Long, Int, Int, Long, Double, Double)](
        alsParams.inputFile, fieldDelimiter = " ")

      val data = input.map(x => (x._2, x._3, x._5))

//      val data = env.fromCollection(smallRatings)
//      val testInput = env.readCsvFile[(Long, Int, Int, Long, Double, Double)](
//        alsParams.testInputFile, fieldDelimiter = " ")
//
//      val test = testInput.map(x => (x._2, x._3, x._5))
//
//      val dataCnt = data.count()
//      val testCnt = test.count()
//
//      println("train data size: " ++ dataCnt.toString)
//      println("test data size: " ++ testCnt.toString)
//
//      val alsParamsTesting = for {
//        l <- Seq(0.1)
//        f <- Seq(10) //,80,100,150,200)
//        iter <- Seq(20)
//        a <- Seq(40)
//      } yield {
//        val currentALS = alsParams.copy(
//          numFactors = f,
//          implicitPrefs = true,
//          blocks = Some(30),
//          lambda = l, iterations = iter,
//          alpha = a)
//        val err = trainAndGetImplicitCost(data, currentALS)
//
//        currentALS.iterations + ", " +
//          currentALS.numFactors + ", " +
//          currentALS.lambda + ", " +
//          currentALS.alpha +
//          "\t\t" + err
//      }
//
//      println("iter,numFact,lambda,alpha")
//      for {param <- alsParamsTesting} yield {
//        println(param)
//      }

      ()
      //       GET THE RANKING
      val test = notRatedUserItemPairs(data)

      val topK = alsParams.topK.getOrElse(100)
      val rankings = trainAndGetRankings(data, test, alsParams, topK)

      rankings.writeAsCsv(alsParams.outputFile, fieldDelimiter = ",")

      env.execute()
    }).getOrElse {
      println("\n\tPlease provide a properties file!")
    }
  }

  def parseALSParams(filePath: String): ALSParams = {
    val parsedArgs = ParameterTool.fromPropertiesFile(filePath)
    val inputFile = parsedArgs.getRequired("ALSInput")
    val testInputFile = parsedArgs.getRequired("ALSTestInput")
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

    val topK = parsedArgs.getInt("RankingTopK", -1) match {
      case -1 => None
      case x => Some(x)
    }

    ALSParams(iterations, blocks, numFactors, lambda, implicitPrefs,
      alpha, inputFile, testInputFile, outputFile, topK)
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
  type IntPair = (Int, Int)

  /**
    * Difference of two [[DataSet]]s.
    * Note, that only works on [[DataSet]]s containing distinct elements.
    *
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
                           als: ALSParams,
                           topK: Int
                         ): DataSet[(Int, Int, Double, Int)] = {

    val model = trainALS(train, als)

    val predictions = model.predict(test)

    Spearman.ranks(predictions, topK)
  }

  def trainAndGetImplicitCost(
                               train: DataSet[(Int, Int, Double)],
                               als: ALSParams
                             ): Double = {
    val model = trainALS(train, als)

    val test = allUserItemPairs(train)
    val predicted = model.predict(test)

    def preference(r: Double): Double = if (r == 0) 0 else 1
    def confidence(r: Double): Double = 1 + als.alpha * r

    val cost =
      train.fullOuterJoin(predicted).where(0, 1).equalTo(0, 1)
        .apply((x: (Int, Int, Double), y: (Int, Int, Double), out: Collector[Double]) => {
          if (x == null && y != null) {
            // there was no rating, so preference = 0, confidence = 1
            val pred = y._3
            out.collect(pred * pred)
          } else if (x != null && y != null) {
            // there was rating, so ...
            val r = x._3
            val pred = y._3
            val p = preference(r)
            val c = confidence(r)

            val d = p - pred
            out.collect(c * d * d)
          }
        }).reduce(_ + _)

    cost.collect().head
  }

  def trainAndGetError(
                        train: DataSet[(Int, Int, Double)],
                        test: DataSet[(Int, Int, Double)],
                        als: ALSParams
                      ): Double = {
    val model = trainALS(train, als)

    val predicted = model.predict(test.map(x => (x._1, x._2)))

    def preference(r: Double): Double = if (r == 0) 0 else 1
    def confidence(r: Double): Double = 1 + als.alpha * r

    val error = test.join(predicted).where(0, 1).equalTo(0, 1)
      .map(x => {
        val r = x._1._3
        val pred = x._2._3

        if (als.implicitPrefs) {
          val p = preference(r)
          val c = confidence(r)

          val d = p - pred
          c * d * d
        } else {
          val d = r - pred
          d * d
        }
      })
      .reduce(_ + _)

    error.collect().head
  }

  def trainALS(
                train: DataSet[(Int, Int, Double)],
                als: ALSParams
              ): ALS = {

    val model = ALS()
      .setNumFactors(als.numFactors)
      .setIterations(als.iterations)
      .setLambda(als.lambda)
      .setImplicit(als.implicitPrefs)
      .setAlpha(als.alpha)
//      .setSeed(42)

    for {b <- als.blocks} yield {
      model.setBlocks(b)
    }

    model.fit(train)

    model
  }
}
