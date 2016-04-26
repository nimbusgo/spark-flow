package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Encoder
import sparkflow.serialization.Hashing

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 2/29/16.
  */
private[sparkflow] class ParallelCollectionDC[T:ClassTag](val data: Seq[T])(implicit tEncoder: Encoder[T]) extends DC[T](Nil) {

  override def computeDataset(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext.createDataset(data)
  }

  override def computeHash() = {
    Hashing.hashString(data.map(_.toString).reduce(_ + _))
  }
}
