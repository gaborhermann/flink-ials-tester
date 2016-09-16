package hu.sztaki.mbalassi.flink.ials.tester.als.spark

import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

import scala.collection.JavaConverters._
import scala.collection.generic.Growable
import java.util.{PriorityQueue => JPriorityQueue}

object SparkUtils {

  type K = Int
  type V = (Int, Int, Double)
  /**
    * Returns the top k (largest) elements for each key from this RDD as defined by the specified
    * implicit Ordering[T].
    * If the number of elements for a certain key is less than k, all of them will be returned.
    *
    * NOTE: this is copied from Spark!
    * @param num k, the number of top elements to return
    * @param ord the implicit ordering for T
    * @return an RDD that contains the top k values for each key
    */
  def topByKey(rdd: RDD[(K,V)])(num: Int)(implicit ord: Ordering[V]): RDD[(K, Array[V])] = {
    rdd.aggregateByKey(new BoundedPriorityQueue[V](num)(ord))(
      seqOp = (queue, item) => {
        queue += item
      },
      combOp = (queue1, queue2) => {
        queue1 ++= queue2
      }
    ).mapValues(_.toArray.sorted(ord.reverse))  // This is an min-heap, so we reverse the order.
  }

  /**
    * Bounded priority queue. This class wraps the original PriorityQueue
    * class and modifies it such that only the top K elements are retained.
    * The top K elements are defined by an implicit Ordering[A].
    */
  private class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
    extends Iterable[A] with Growable[A] with Serializable {

    private val underlying = new JPriorityQueue[A](maxSize, ord)

    override def iterator: Iterator[A] = underlying.iterator.asScala

    override def size: Int = underlying.size

    override def ++=(xs: TraversableOnce[A]): this.type = {
      xs.foreach { this += _ }
      this
    }

    override def +=(elem: A): this.type = {
      if (size < maxSize) {
        underlying.offer(elem)
      } else {
        maybeReplaceLowest(elem)
      }
      this
    }

    override def +=(elem1: A, elem2: A, elems: A*): this.type = {
      this += elem1 += elem2 ++= elems
    }

    override def clear() { underlying.clear() }

    private def maybeReplaceLowest(a: A): Boolean = {
      val head = underlying.peek()
      if (head != null && ord.gt(a, head)) {
        underlying.poll()
        underlying.offer(a)
      } else {
        false
      }
    }
  }


}
