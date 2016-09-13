package hu.sztaki.mbalassi.ials.tester.als

import org.scalatest._

import scala.language.postfixOps
import org.apache.flink.api.scala._
import hu.sztaki.mbalassi.flink.ials.tester.als.flink.FlinkALS._

class FlinkALSTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfter {

  var env: ExecutionEnvironment = null
  val ratings = Seq((1,50,1.0), (2,50,1.0), (8,10,1.0), (8,20,1.0))

  before {
    env = ExecutionEnvironment.getExecutionEnvironment
  }

  behavior of "The Flink ALS tester"

  it should "calculate difference of two datasets" in {
//    val env = ExecutionEnvironment.getExecutionEnvironment

    val a = env.fromElements((1,1), (2,2), (3,3), (4,4))
    val b = env.fromElements((3,3), (1,1))

    val expectedDiff = Seq((2,2), (4,4))

    minus(a, b).collect() should be (expectedDiff)
  }

  it should "calculate all pairs of (user,item)" in {
    val ratingsDS = env.fromCollection(ratings)

    val expectedAllPairs = Seq(
      (1,10),
      (1,20),
      (1,50),
      (2,10),
      (2,20),
      (2,50),
      (8,10),
      (8,20),
      (8,50)
    ).sortBy(identity)

    allUserItemPairs(ratingsDS).collect().sortBy(identity) should be (expectedAllPairs)
  }

  it should "calculate not rated pairs of (user,item)" in {
    val ratingsDS = env.fromCollection(ratings)

    val expectedAllPairs = Seq(
      (1,10),
      (1,20),
      (2,10),
      (2,20),
      (8,50)
    ).sortBy(identity)

    notRatedUserItemPairs(ratingsDS).collect().sortBy(identity) should be (expectedAllPairs)
  }
  
}
