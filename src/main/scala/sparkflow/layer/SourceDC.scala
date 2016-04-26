package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import sparkflow.serialization.Hashing
import org.apache.spark.sql.{SQLContext, Encoder}


/**
  * Created by ngoehausen on 3/23/16.
  */
class SourceDC[T: ClassTag](val source: String, val sourceFunc: SparkContext => RDD[T], val sourceType: String)(implicit tEncoder: Encoder[T]) extends DC[T](Nil)  {

  override def computeDataset(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sourceFunc(sc).toDS()
  }

  override def computeHash() = {
    Hashing.hashString(s"$sourceType:$source")
  }

}