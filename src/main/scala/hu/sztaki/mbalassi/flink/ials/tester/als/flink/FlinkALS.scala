package hu.sztaki.mbalassi.flink.ials.tester.als.flink

import hu.sztaki.mbalassi.flink.ials.tester.als.correlation.Spearman
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.recommendation. ALS

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
//    val res = model.predict(test).print

    val res=model.predict(test) //results from the prediction

    val ratings=res.collect().toArray //transform the dataset to an array

    val sp=Spearman
    val ranks=sp.ranks(ratings, numCustomers, numStores) //compute the ranks per customers

    val corr=sp.averageCorr(ranks, ranks)
  }

}
