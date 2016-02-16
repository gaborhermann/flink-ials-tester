package hu.sztaki.mbalassi.flink.ials.tester.als.spark

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation._

object SparkALS {
  def main(args: Array[String]) {
    val paramLoc = "src/main/resources/benchmark.properties"

    val parsedArgs = ParameterTool.fromPropertiesFile(paramLoc)
    val master = parsedArgs.getRequired("sparkMaster")
    val inputFile = parsedArgs.getRequired("ALSInput")
    val numCustomers = parsedArgs.getRequired("ALSNumCustomers").toInt
    val numStores = parsedArgs.getRequired("ALSNumStores").toInt
    val iterations = parsedArgs.getRequired("ALSIterations").toInt
    val numFactors = parsedArgs.getRequired("ALSNumFactors").toInt
    val lambda = parsedArgs.getRequired("ALSLambda").toDouble
    val blocks = parsedArgs.getRequired("ALSBlocks").toInt
    val implicitPrefs = parsedArgs.getRequired("ImplicitPrefs").equals("true")

    val conf = new SparkConf(true).setMaster(master)
      .setAppName("Spark ALS")

    val sc = new SparkContext(conf)

    // Read and parse the input data
    val input = sc.textFile(inputFile)
    .map(s => Rating(s.split(",")(0).toInt, s.split(",")(1).toInt, 1.0))

    var model : MatrixFactorizationModel = null

    // Create a model using MLlib
    if (implicitPrefs) {
      model = ALS.trainImplicit(input, numFactors, iterations, lambda, blocks)
    } else {
      model = ALS.train(input, numFactors, iterations, lambda, blocks)
    }

    val test = sc.parallelize(for {x <- 0 to numCustomers; y <- 0 to numStores} yield (x, y))

    model.predict(test).foreach(r => println(r.user, r.product, r.rating))
  }
}
