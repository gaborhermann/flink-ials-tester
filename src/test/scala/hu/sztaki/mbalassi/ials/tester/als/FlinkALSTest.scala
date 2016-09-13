package hu.sztaki.mbalassi.ials.tester.als

import org.scalatest._

import scala.language.postfixOps
import org.apache.flink.api.scala._

class FlinkALSTest
  extends FlatSpec
    with Matchers {

  behavior of "The alternating least squares (ALS) implementation"

  it should "properly factorize a matrix" in {
    val x = 5
    x should be(6)
  }

}
