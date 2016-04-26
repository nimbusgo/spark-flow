package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag
import sparkflow.serialization.Hashing

/**
  * Created by ngoehausen on 4/19/16.
  */
class MultiInputDC[T: ClassTag, K:ClassTag](inputs: Seq[DC[_ <: Product2[K, _]]],
                                                     f: (Seq[Dataset[_ <: Product2[K, _]]]) => Dataset[T])
                                           (implicit tEncoder: Encoder[T])
extends DC[T](inputs){

  override def computeHash() = {
    Hashing.hashString(inputs.map(_.getHash).mkString("") + Hashing.hashClass(f))
  }

  override def computeDataset(sc: SparkContext) = {
    f(inputs.map(_.getDataset(sc)))
  }

}
