package hu.sztaki.mbalassi.flink.ials.tester.als.spark

import hu.sztaki.mbalassi.flink.ials.tester.als.flink.FlinkALS
import hu.sztaki.mbalassi.flink.ials.tester.als.flink.FlinkALS.ALSParams
import hu.sztaki.mbalassi.flink.ials.tester.utils.Utils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD

object SparkALS {

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

  def main(args: Array[String]) {

    val propFileOption = Utils.parameterCheck(args)

    propFileOption.map(propFile => {
      val alsParams = FlinkALS.parseALSParams(propFile)

      // initialize Spark context
      // todo parse from params
      val sparkMaster = "local[1]"
      val sparkCheckpointDir = "/home/ghermann/sparkCheckpoint"

      val conf = new SparkConf(true).setMaster(sparkMaster)
        .setAppName("Spark ALS")

      val sc = new SparkContext(conf)
      sc.setCheckpointDir(sparkCheckpointDir)

      // Read and parse the input data:
      // last fm schema: 'time','user','item','id','score','eval'
      def parseLastFMCsv(lines: RDD[String]): RDD[(Int, Int, Double)] = {
        lines.map(line => {
          line.split(" ") match {
            case Array(_, userStr, itemStr, _, scoreStr, _) =>
              (userStr.toInt, itemStr.toInt, scoreStr.toDouble)
          }
        })
      }

//      val input = sc.textFile(alsParams.inputFile)
//      val data = parseLastFMCsv(input)
//
//      val testInput = sc.textFile(alsParams.testInputFile)
//      val test = parseLastFMCsv(testInput)
//
//      val dataCnt = data.count()
//      val testCnt = test.count()
//
//      println("train data size: " ++ dataCnt.toString)
//      println("test data size: " ++ testCnt.toString)

      val data = sc.parallelize(smallRatings)

      val alsParamsTesting = for {
        l <- Seq(0)
        f <- Seq(20)
        iter <- Seq(10)
      } yield {
        val currentALS = alsParams.copy(
          numFactors = f,
          implicitPrefs = false,
          blocks = Some(30),
          lambda = l, iterations = iter)
        val err = trainAndGetImplicitCost(data, currentALS)

        currentALS.iterations + ", " +
          currentALS.numFactors + ", " +
          currentALS.lambda + ", " +
          currentALS.alpha +
          "\t\t" + err
      }

      println("iter,numFact,lambda,alpha")
      for {param <- alsParamsTesting} yield {
        println(param)
      }
//      val test = notRatedUserItemPairs(data)
//
//      val rankings = trainAndGetRankings(data, test, alsParams)
//
//      // todo optimize: only calculate topK
//      // filter rankings, only show top k
//      val topKRankings = (for {k <- alsParams.topK}
//        yield {
//          rankings.filter(x => x._4 <= k)
//        })
//        .getOrElse(rankings)
//
//      topKRankings
//          .map{ case (u,i,s,r) =>
//            u.toString ++ "," ++ i.toString ++ "," ++ s.toString ++ "," ++ r.toString
//          }
//        .saveAsTextFile(alsParams.outputFile)
//
//      ()
    }).getOrElse(println("\n\tPlease provide a param file"))
  }

  def allUserItemPairs(data: RDD[(Int, Int, Double)]): RDD[(Int, Int)] = {
    val users = data.map(_._1).distinct()
    val items = data.map(_._2).distinct()

    users.cartesian(items)
  }

  def notRatedUserItemPairs(data: RDD[(Int, Int, Double)]): RDD[(Int, Int)] = {
    val rated = data.map(x => (x._1, x._2)).distinct()
    val all = allUserItemPairs(data)

    minus(all, rated)
  }

  // todo make generic
  type IntPair = (Int, Int)

  /** Note, that only works on [[RDD]]s containing distinct elements.
    */
  def minus(a: RDD[IntPair], b: RDD[IntPair]): RDD[IntPair] = {
    a.map((_, ())).fullOuterJoin(b.map((_, ()))).flatMap {
      case (pair, (Some(_), None)) => Some(pair)
      case _ => None
    }
  }

  def trainAndGetRankings(
                           train: RDD[(Int, Int, Double)],
                           test: RDD[(Int, Int)],
                           als: ALSParams
                         ): RDD[(Int, Int, Double, Int)] = {

    val model = trainALS(train, als)

    val predictions = model
      .predict(test)
      .map { case Rating(u, i, r) => (u, i, r) }

    ranks(predictions, topK = als.topK.getOrElse(100))
  }

  def ranks(ratings: RDD[(Int, Int, Double)], topK: Int): RDD[(Int, Int, Double, Int)] = {
    import SparkUtils._

    val ranks = topByKey(ratings.keyBy(_._1))(topK)(Ordering.by(_._3))
      .flatMap { case (u, topRatings) => {
        for {
          ranking <- Seq.range(0, topRatings.length)
        } yield {
          topRatings(ranking) match {
            case (_,i,score) => (u,i,score,ranking)
          }
        }
      }}

    ranks
  }

  def trainAndGetImplicitCost(
                        train: RDD[(Int, Int, Double)],
                        als: ALSParams
                      ): Double = {
    val model = trainALS(train, als)

    val test = allUserItemPairs(train)

    val predicted = model
      .predict(test)
      .map { case Rating(u, i, r) => ((u, i), r) }

    val keyedTrain = train.map { case (u,i,r) => ((u,i),r)}

    def preference(r: Double): Double = if (r == 0) 0 else 1
    def confidence(r: Double): Double = 1 + als.alpha * r

    val error = keyedTrain.fullOuterJoin(predicted)
        .map {
          case (_, (Some(r), Some(pred))) =>
            confidence(r) * (1 - pred) * (1 - pred)
          case (_, (None, Some(pred))) =>
            1 * (0 - pred)
        }
      .reduce(_ + _)

    error
  }

  def trainAndGetError(
                        train: RDD[(Int, Int, Double)],
                        test: RDD[(Int, Int, Double)],
                        als: ALSParams
                      ): Double = {
    val model = trainALS(train, als)

    val predicted = model
      .predict(test.map(x => (x._1, x._2)))
      .map { case Rating(u, i, r) => ((u, i), r) }

    val testKeyed = test.map { case (u, i, r) => ((u, i), r) }

    def preference(r: Double): Double = if (r == 0) 0 else 1
    def confidence(r: Double): Double = 1 + als.alpha * r

    val error = testKeyed.join(predicted)
      .map { case (_, (r, pred)) => {
        if (als.implicitPrefs) {
          val p = preference(r)
          val c = confidence(r)

          val d = p - pred
          c * d * d
        } else {
          val d = r - pred
          d * d
        }
      }
      }
      .reduce(_ + _)

    error
  }

  def trainALS(
                train: RDD[(Int, Int, Double)],
                als: ALSParams
              ): MatrixFactorizationModel = {
    // todo maybe use num of blocks
    val data = train.map { case (u, i, r) => Rating(u, i, r) }

    val model: MatrixFactorizationModel =
      if (als.implicitPrefs) {
        ALS.trainImplicit(data, als.numFactors, als.iterations, als.lambda, als.alpha)
      } else {
        ALS.train(data, als.numFactors, als.iterations, als.lambda)
      }

    model
  }
}
