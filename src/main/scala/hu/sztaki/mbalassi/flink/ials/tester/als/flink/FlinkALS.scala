package hu.sztaki.mbalassi.flink.ials.tester.als.flink

import hu.sztaki.mbalassi.flink.ials.tester.als.correlation.Spearman
import hu.sztaki.mbalassi.flink.ials.tester.utils.Utils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object FlinkALS {

  case class ALSParams(
                        iterations: Int,
                        blocks: Int,
                        numFactors: Int,
                        lambda: Double,
                        implicitPrefs: Boolean,
                        alpha: Double
                      )

  def main(args: Array[String]) {

    val propFile = Utils.parameterCheck(args)

    val parsedArgs = ParameterTool.fromPropertiesFile(propFile)
    val inputFile = parsedArgs.getRequired("ALSInput")
    val testInputFile = parsedArgs.getRequired("ALSTestInput")
    val iterations = parsedArgs.getRequired("ALSIterations").toInt
    val numFactors = parsedArgs.getRequired("ALSNumFactors").toInt
    val lambda = parsedArgs.getRequired("ALSLambda").toDouble
    val blocks = parsedArgs.getRequired("ALSBlocks").toInt
    val implicitPrefs = parsedArgs.getRequired("ALSImplicitPrefs").equals("true")
    val alpha = parsedArgs.getRequired("ALSAlpha").toInt

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read and parse the input data: (timestamp, user, store == artist, 1)
    val input = env.readCsvFile[(Long, Int, Int, Int)](inputFile, fieldDelimiter = " ")
      .map(tuple => (tuple._2, tuple._3, 1.0))

    val customers = input.map(_._1).distinct()
    val stores = input.map(_._2).distinct()

    val numCustomers = customers.reduce((x1, x2) => (if (x1 > x2) x1 else x2)).collect()(0)
    val numStores = stores.reduce((x1, x2) => (if (x1 > x2) x1 else x2)).collect()(0)

    println("-------------------------")
    println("NumCustomers: " + numCustomers)
    println("NumStores: " + numStores)
    println("-------------------------")
  }

  // todo test
  def allUserItemPairs(data: DataSet[(Int,Int,Double)]): DataSet[(Int,Int)] = {
    val users = data.map(_._1).distinct()
    val items = data.map(_._2).distinct()

    users cross items
  }

  // todo test
  def notRatedUserItemPairs(data: DataSet[(Int,Int,Double)]): DataSet[(Int,Int)] = {
    val rated = data.map(x => (x._1,x._2)).distinct()
    val all = allUserItemPairs(data)

    minus(all, rated)
  }

  // todo test
  def minus[A](a: DataSet[A], b: DataSet[A]): DataSet[A] = {
    a.fullOuterJoin(b).where(x => x).equalTo(x => x).apply(
      (x: A, y: A, out: Collector[A]) => {
        if (x != null && y == null) {
          out.collect(x)
        }
      }
    )
  }

  def trainAndGetRankings(
                           train: DataSet[(Int, Int, Double)],
                           test: DataSet[(Int, Int, Double)],
                           als: ALSParams
                         ): DataSet[(Int, Int, Double, Int)] = {
    val model = ALS()
      .setNumFactors(als.numFactors)
      .setIterations(als.iterations)
      .setLambda(als.lambda)
      .setBlocks(als.blocks)
      .setImplicit(als.implicitPrefs)
      .setAlpha(als.alpha)

    model.fit(train)

    val testToGivePreds = test.map(x => (x._1,x._2))

    val predictions = model.predict(testToGivePreds)

    Spearman.ranks(predictions)
  }

}
