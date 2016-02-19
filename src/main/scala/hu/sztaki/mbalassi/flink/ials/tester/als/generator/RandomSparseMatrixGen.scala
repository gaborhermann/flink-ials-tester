package hu.sztaki.mbalassi.flink.ials.tester.als.generator

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * Generates uniformly random matrix values for the desired dimensions
  * with the given sparseness.
  */
object RandomSparseMatrixGen {
  def main(args: Array[String]) {

    val paramLoc = "src/main/resources/benchmark.properties"

    val parameters = ParameterTool.fromPropertiesFile(paramLoc)
    val output = parameters.getRequired("ALSInput")
    val numCustomers = parameters.getRequired("ALSNumCustomers").toInt
    val numStores = parameters.getRequired("ALSNumStores").toInt
    val sparseness = parameters.getRequired("ALSSparseness").toDouble

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new GeneratorSource(numCustomers, numStores, sparseness))
      .writeAsCsv(output, WriteMode.OVERWRITE, "\n", ",")

    env.execute("Flink uniformly random sparse matrix")

  }

  class GeneratorSource(val numCustomers : Int, val numStores : Int, val sparseness : Double)
    extends RichParallelSourceFunction[(Int, Int)]{

    var parallelism : Int = 0
    var congruence : Int = 0
    var random : Random = null

    override def open(parameters: Configuration): Unit = {
      parallelism = getRuntimeContext.getNumberOfParallelSubtasks
      congruence = getRuntimeContext.getIndexOfThisSubtask
      random = new Random(congruence)
    }

    override def cancel(): Unit = {}

    override def run(sourceContext: SourceContext[(Int, Int)]): Unit = {
      var current = congruence
      while (current < numCustomers){
        for (i <- 0 to numStores - 1){
          if (random.nextDouble() > sparseness){
            sourceContext.collect((current, i))
          }
        }
        current += parallelism
      }
    }
  }
}
