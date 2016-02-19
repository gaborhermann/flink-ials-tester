package hu.sztaki.mbalassi.flink.ials.tester.als.generator

import java.util

import com.github.rnowling.bps.datagenerator._
import com.github.rnowling.bps.datagenerator.datamodels._
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ProductCategory
import com.github.rnowling.bps.datagenerator.framework.SeedFactory
import hu.sztaki.mbalassi.flink.ials.tester.FlinkTransaction
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.language.implicitConversions

object BPSDataGen {

  val paramLoc = "src/main/resources/benchmark.properties"

  val parameters = ParameterTool.fromPropertiesFile(paramLoc)
  val output = parameters.getRequired("ALSInput")
  val numCustomers = parameters.getRequired("ALSNumCustomers").toInt
  val numStores = parameters.getRequired("ALSNumStores").toInt
  val simulationLength = parameters.getRequired("BPSSimulationLength").toDouble

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.registerType(classOf[com.github.rnowling.bps.datagenerator.datamodels.Store])

    val inputData = new DataLoader().loadData()
    val seedFactory = new SeedFactory(42)

    val storeGenerator = new StoreGenerator(inputData, seedFactory)
    val stores = for (i <- 1 to numStores) yield storeGenerator.generate
    val storesDataSet = env.fromCollection(stores)

    val customerGenerator = new CustomerGenerator(inputData, stores, seedFactory)
    val customers = for (i <- 1 to numCustomers) yield customerGenerator.generate

    // Generate transactions
    val transactions = env.fromCollection(customers)
      .flatMap(new TransactionGeneratorFlatMap(inputData.getProductCategories))
      .withBroadcastSet(storesDataSet, "stores")

    // Generate unique product IDs
    val productsWithIndex = transactions.flatMap( _.getProducts)
      .map(_.toString)
      .distinct
      .zipWithUniqueId

    // Generate the customer-product pairs
    val CustomerAndProductIDS = transactions.flatMap(new TransactionUnZipper)
      .join(productsWithIndex)
      .where(_._2)
      .equalTo(_._2)
      .map(pair => (pair._1._1, pair._2._1))
      .distinct

    CustomerAndProductIDS.writeAsCsv(output, "\n", ",", WriteMode.OVERWRITE)

    //Print stats on the generated dataset
    val numProducts = productsWithIndex.count
    val filledFields = CustomerAndProductIDS.count
    val sparseness = 1 - (filledFields.toDouble / (numProducts * numCustomers))

    println("Generated bigpetstore stats")
    println("---------------------------")
    println("Customers:\t" + numCustomers)
    println("Stores:\t\t" + numStores)
    println("simLength:\t" + simulationLength)
    println("Products:\t" + numProducts)
    println("sparse:\t\t" + sparseness)

  }

  implicit def toFlink(t : Transaction) : FlinkTransaction = {new FlinkTransaction(t)}

  class TransactionGeneratorFlatMap(products : util.Collection[ProductCategory])
    extends RichFlatMapFunction[Customer, FlinkTransaction] {

    var profile : PurchasingProfile = null
    var seedFactory : SeedFactory = null
    var stores : util.List[Store] = null

    override def open(parameters: Configuration): Unit = {
      val seed = getRuntimeContext.getIndexOfThisSubtask
      seedFactory = new SeedFactory(seed)
      profile = new PurchasingProfileGenerator(products, seedFactory).generate
      stores = getRuntimeContext.getBroadcastVariable[Store]("stores")
    }

    override def flatMap(customer: Customer, collector: Collector[FlinkTransaction]): Unit =  {
      val transGen = new TransactionGenerator(customer, profile, stores, products, seedFactory)
      var transaction = transGen.generate

      while (transaction.getDateTime < simulationLength) {
        collector.collect(transaction)
        transaction = transGen.generate
      }
    }
  }

  class TransactionUnZipper extends FlatMapFunction[FlinkTransaction, (Int, String)] {
    override def flatMap(t: FlinkTransaction, collector: Collector[(Int, String)]): Unit = {
      val it = t.getProducts.iterator()
      while (it.hasNext){
        collector.collect(t.getCustomer.getId, it.next.toString)
      }
    }
  }

}