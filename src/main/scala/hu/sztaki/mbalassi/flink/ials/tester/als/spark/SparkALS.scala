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

  def main(args: Array[String]) {

    val propFileOption = Utils.parameterCheck(args)

    propFileOption.map(propFile => {
      val alsParams = FlinkALS.parseALSParams(propFile)

      // initialize Spark context
      // todo parse from params
      val sparkMaster = "local[4]"
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

      val input = sc.textFile(alsParams.inputFile)
      val testInput = sc.textFile(alsParams.testInputFile)

      val data = parseLastFMCsv(input)
      val test = parseLastFMCsv(testInput)

      val dataCnt = data.count()
      val testCnt = test.count()

      println("train data size: " ++ dataCnt.toString)
      println("test data size: " ++ testCnt.toString)

      val alsParamsTesting = for {
        l <- Seq(0.1)
        f <- Seq(20) //,80,100,150,200)
        iter <- Seq(50)
      } yield {
        val currentALS = alsParams.copy(
          numFactors = f,
          implicitPrefs = true,
          blocks = Some(30),
          lambda = l, iterations = iter)
        val err = trainAndGetError(data, test, currentALS)

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

      ()
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

    ranks(predictions)
  }

  def ranks(ratings: RDD[(Int, Int, Double)]): RDD[(Int, Int, Double, Int)] = {
    //    val res = ratings.groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup{
    //      (in, out: Collector[(Int, Int, Double, Int)]) =>
    //        var rank = 1
    //        for(t <- in){
    //          out.collect(t._1, t._2, t._3, rank)
    //          rank += 1
    //        }
    //    }
    //    res
    // todo
    null
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
