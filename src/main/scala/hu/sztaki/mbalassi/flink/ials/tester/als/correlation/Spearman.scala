package hu.sztaki.mbalassi.flink.ials.tester.als.correlation

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.DataSet
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
  * Example:
  * val res=model.predict(test) //results from the prediction
  *
  * val sp=Spearman
  * val ranks=sp.ranks(ratings) //compute the ranks per customers
  *
  * val corr=sp.corr(ranks, ranks)
  */

object Spearman {
  def ranks(ratings: DataSet[(Int, Int, Double)], topK: Int):
  DataSet[(Int, Int, Double, Int)] = {

    val res = ratings.groupBy(0).sortGroup(2, Order.DESCENDING)
      .first(topK)
      .reduceGroup {
        (in, out: Collector[(Int, Int, Double, Int)]) =>
          var rank = 1

          for (t <- in) {
            out.collect(t._1, t._2, t._3, rank)
            rank += 1
          }
        //        val it = in.toIterator
        //        while (it.hasNext && rank <= topK) {
        //          val t = it.next()
        //
        //          out.collect(t._1, t._2, t._3, rank)
        //          rank += 1
        //        }
      }
    res
  }

  def corr(dataSet1: DataSet[(Int, Int, Double, Int)], dataSet2: DataSet[(Int, Int, Double, Int)]): (Double, Double) = {

    val correlations = dataSet1.join(dataSet2).where(0, 1).equalTo(0, 1) {
      (data1, data2, out: Collector[(Int, Int, Int, Int)]) =>
        out.collect(data1._1, data1._2, data1._4, data2._4)
    }.groupBy(0).reduceGroup {
      (in, out: Collector[Double]) =>
        var distsquare = 0.0
        var numItem = 0
        for (t <- in) {
          val dist = Math.abs(t._3 - t._4)
          distsquare += dist * dist
          numItem += 1
        }
        val corr = 1 - 6 * distsquare / (numItem * (numItem * numItem - 1))
        out.collect(corr)
    }

    val num = correlations.count()
    val sum = correlations.reduce {
      (x1, x2) => x1 + x2
    }.collect()(0)

    val corrRes = sum / num
    val dev = correlations.map {
      x => (x - corrRes) * (x - corrRes)
    }.reduce {
      (x1, x2) => x1 + x2
    }.collect()(0)
    (corrRes, Math.sqrt(dev))
  }

}