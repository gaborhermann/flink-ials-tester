package hu.sztaki.mbalassi.flink.ials.tester.als.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.recommendation.{Spearman, ALS}

import scala.collection.mutable.ArrayBuffer

object FlinkALS {
  def main(args: Array[String]) {

    val paramLoc = "src/main/resources/benchmark.properties"

    val parsedArgs = ParameterTool.fromPropertiesFile(paramLoc)
    val inputFile = parsedArgs.getRequired("ALSInput")
    val numCustomers = parsedArgs.getRequired("ALSNumCustomers").toInt
    val numStores = parsedArgs.getRequired("ALSNumStores").toInt
    val iterations = parsedArgs.getRequired("ALSIterations").toInt
    val numFactors = parsedArgs.getRequired("ALSNumFactors").toInt
    val lambda = parsedArgs.getRequired("ALSLambda").toDouble
    val blocks = parsedArgs.getRequired("ALSBlocks").toInt
    val implicitPrefs = parsedArgs.getRequired("ImplicitPrefs").equals("true")

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read and parse the input data
    val input = env.readCsvFile[(Int, Int)](inputFile)
      .map(pair => (pair._1, pair._2, 1.0))

    // Create a model using FlinkML
    val model = ALS()
      .setNumFactors(numFactors)
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(blocks)
      .setImplicit(implicitPrefs)

    model.fit(input)

    val test = env.fromCollection(for {x <- 0 to numCustomers; y <- 0 to numStores} yield (x, y))
    val res = model.predict(test)//.print

//    val resA = new ArrayBuffer[(Int, Int, Double)]()
//    res.map( x => resA+=x)
    val resA = res.collect().toArray
    println(resA.size)
    val sp = Spearman
    val ratings = resA.toArray
//    val numUser = numCustomers
//    val numItem = numStores
//    val ranks = new ArrayBuffer[Array[(Double, Int)]](numUser)
//    val ranksTemp = new ArrayBuffer[Array[(Double, Int)]](numUser)
//    for(i <- 0 to numUser-1){
//      ranks += new Array[(Double, Int)](numItem)
//      ranksTemp += new Array[(Double, Int)](numItem)
//    }
//    for(i <- ratings.indices){
//      val user = ratings(i)._1
//      val item = ratings(i)._2
//      println(user, item, ratings(i)._3)
//      ranks(user)(item) = new Tuple2(ratings(i)._3, item)
//    }
//
//    println(ranks(1)(1)._1)
//    ranksTemp(0) = ranks(0).sortBy(_._1)
//    for(i <- ranksTemp.indices){
//      ranksTemp(i) = ranks(i).sortBy(_._1)
//    }
    val ranks = sp.ranks(ratings, numCustomers, numStores)
    val fres = sp.averageCorr(ranks, ranks)
    println(fres)
  }

}
