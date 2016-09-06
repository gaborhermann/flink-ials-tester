package hu.sztaki.mbalassi.flink.ials.tester.als.test.ials

import org.apache.spark.mllib.recommendation._
import org.apache.spark.{SparkConf, SparkContext}

object SparkALSTest {

  def runALSTest(train: Seq[(Int, Int, Double)],
                 testData: Seq[(Int, Int, Double)],
                 rank: Int,
                 lambda: Double,
                 numIter: Int,
                 alpha: Double): Array[Rating] = {

    val conf = new SparkConf()
      .setAppName("Spark ALS")
      .setMaster("local")

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("/home/ghermann/sparkCheckpoint")

    // Load and parse the data
    val data = sc.parallelize(train)
    val ratings = data.map { case (user, item, rate) =>
      Rating(user, item, rate)
    }

    // Build the recommendation model using ALS
    // model for explicit feedback ALS
    //    val model = ALS.train(ratings, rank, 200, lambda)
    // model for implicit feedback ALS
    val model = ALS.trainImplicit(ratings, rank, numIter, lambda, alpha)
    //    (ratings, rank, 200, lambda)

    val test = sc.parallelize(testData)
      .map { case (u, i, r) =>
        (u, i)
      }

    val res = model.predict(test)

    res.collect()
  }

}
