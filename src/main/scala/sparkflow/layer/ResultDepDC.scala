package sparkflow.layer

import org.apache.spark.SparkContext
import sparkflow.serialization.Hashing._
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

/**
  * ResultDependentDistributedCollection
  */
class ResultDepDC[U:ClassTag, T:ClassTag, V: ClassTag]
(val prev: DC[T], dr: DR[U],f: (T,U) => V)(implicit vEncoder: Encoder[V]) extends DC[V](Seq(prev, dr)) {

  override def computeDataset(sc: SparkContext) = {
    val result = dr.get(sc)
    prev.getDataset(sc).mapPartitions(iterator => {
      iterator.map(t => f(t, result))
    })

  }

  override def computeHash() = {
    hashString(prev.getHash + dr.getHash + hashClass(f))
  }

}
