package hu.sztaki.mbalassi.flink.ials.tester.als.correlation

import scala.collection.mutable.ArrayBuffer

/**
  * Example:
  * val res=model.predict(test) //results from the prediction

    val ratings=res.collect().toArray //transform the dataset to an array

    val sp=Spearman
    val ranks=sp.ranks(ratings, numCustomers, numStores) //compute the ranks per customers

    val corr=sp.averageCorr(ranks, ranks)
  */
object Spearman {
  def ranks(ratings: Array[(Int, Int, Double)], numUser: Int, numItem: Int):
  Array[Array[(Double, Int)]] ={
    val ranks = new ArrayBuffer[Array[(Double, Int)]](numUser+1)
    val ranksTemp = new ArrayBuffer[Array[(Double, Int)]](numUser+1)
    for(i <- 0 to numUser){
      ranks += new Array[(Double, Int)](numItem+1)
      ranksTemp += new Array[(Double, Int)](numItem+1)
    }
    for(i <- ratings.indices){
      val user = ratings(i)._1
      val item = ratings(i)._2
      println(i, ratings.length, user, numUser, item, numItem)
      ranks(user)(item) = new Tuple2(ratings(i)._3, item)
    }

    for(i <- ranksTemp.indices){
      ranksTemp(i) = ranks(i).sortBy(_._1)
    }

    for(i <- ranks.indices){
      for(j <- 0 to numItem-1) {
        ranks(i)(ranksTemp(i)(j)._2) = new Tuple2(ranksTemp(i)(j)._1, j)
      }
    }
    ranks.toArray
  }

  def corr(data1: Array[(Double, Int)], data2: Array[(Double, Int)]): Double = {
    var distsquare = 0.0
    for(i <- data1.indices){
      val dist = Math.abs(data1(i)._2-data2(i)._2)
      distsquare += dist*dist
    }
    val numItem = data1.length
    1-6*distsquare/(numItem*(numItem*numItem-1))
  }

  /**
    *
    * @param data1 The average of the correlations per user
    * @param data2 The deviation of the correlations
    * @return
    */
  def averageCorr(data1: Array[Array[(Double, Int)]], data2: Array[Array[(Double, Int)]]):
  (Double, Double) ={
    var corrSum = 0.0
    var dev = 0.0
    val corrs = new ArrayBuffer[Double]()
    for(i <- data1.indices){
      val correlation = corr(data1(i), data2(i))
      corrSum += correlation
      corrs += correlation
    }

    corrSum = corrSum / data1.length

    for(i <- corrs.indices){
      dev += (corrs(i)-corrSum)*(corrs(i)-corrSum)
    }

    (corrSum, Math.sqrt(dev))
  }
}