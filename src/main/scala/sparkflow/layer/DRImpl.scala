package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import sparkflow.serialization.Hashing._

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 4/19/16.
  */
class DRImpl[T: ClassTag, U: ClassTag](prev: DC[T], func: Dataset[T] => U) extends DR[U](prev){

  private var result: U = _

  override def get(sc: SparkContext) = {
    if (result == null){
      result = computeResult(sc)
    }
    result
  }

  private def computeResult(sc: SparkContext) = {
    func(prev.getDataset(sc))
  }

  override def computeHash() = {
    hashString(prev.getHash + hashClass(func))
  }

}
