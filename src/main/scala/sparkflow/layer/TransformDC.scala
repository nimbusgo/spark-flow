package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import sparkflow.serialization.Hashing._

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/20/16.
  */
class TransformDC[U:ClassTag, T:ClassTag]
(val prev: DC[T],
 f: Dataset[T] => Dataset[U],
 hashTarget: AnyRef)(implicit tEncoder: Encoder[T], uEncoder: Encoder[U]) extends DC[U](Seq(prev)) {

  override def computeDataset(sc: SparkContext) = {
    f(prev.getDataset(sc))
  }

  override def computeHash() = {
    hashString(prev.getHash + hashClass(hashTarget))
  }
}
