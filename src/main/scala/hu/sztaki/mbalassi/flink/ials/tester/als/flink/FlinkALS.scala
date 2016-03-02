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
//    val numCustomers = parsedArgs.getRequired("ALSNumCustomers").toInt
//    val numStores = parsedArgs.getRequired("ALSNumStores").toInt
    val iterations = parsedArgs.getRequired("ALSIterations").toInt
    val numFactors = parsedArgs.getRequired("ALSNumFactors").toInt
    val lambda = parsedArgs.getRequired("ALSLambda").toDouble
    val blocks = parsedArgs.getRequired("ALSBlocks").toInt
    val implicitPrefs = parsedArgs.getRequired("ImplicitPrefs").equals("true")

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read and parse the input data: (timestamp, user, store == artist, 1)
    val input = env.readCsvFile[(Long, Int, Int, Int)](inputFile, fieldDelimiter = " ")
      .map(tuple => (tuple._2, tuple._3, 1.0))

    val customers = input.map(_._1).distinct()
    val stores = input.map(_._2).distinct()

    val numCustomers = customers.reduce((x1, x2) => (if(x1 > x2) x1 else x2)).collect()(0)
    val numStores = stores.reduce((x1, x2) => (if(x1 > x2) x1 else x2)).collect()(0)

    println("-------------------------")
    println("NumCustomers: " + numCustomers)
    println("NumStores: " + numStores)
    println("-------------------------")

    // Create a model using FlinkML
    val model = ALS()
      .setNumFactors(numFactors)
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(blocks)
      .setImplicit(implicitPrefs)

    model.fit(input)

    val test = customers cross stores
    val res = model.predict(test)

    val sp=Spearman

      val ranks = sp.ranks(res)
      val averageCorr = sp.corr(ranks, ranks)
      println(averageCorr)
  }

}
