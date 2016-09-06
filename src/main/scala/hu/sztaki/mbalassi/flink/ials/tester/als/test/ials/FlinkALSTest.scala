package hu.sztaki.mbalassi.flink.ials.tester.als.test.ials

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS._
import org.apache.flink.ml.recommendation._

object FlinkALSTest {

  def runALSTest(train: Seq[(Int, Int, Double)],
                 testData: Seq[(Int, Int, Double)],
                 rank: Int,
                 lambda: Double,
                 numIter: Int,
                 alpha: Double): Seq[(Int, Int, Double)] = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromCollection(train)
    val test = env.fromCollection(testData)

    // Build the recommendation model using ALS

    val als = ALS()
      .setIterations(numIter)
      .setNumFactors(rank)
      // implicit
      .setImplicit(true)
      .setAlpha(alpha)
      .setLambda(lambda)

    val parameters = ParameterMap()

    als.fit(data, parameters)

    // Evaluate the model on rating data
    val usersProducts = test.map(r => (r._1, r._2))

    val predictions = als.predict(usersProducts)
    predictions.collect()
  }

}
