package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext, Encoder}

/**
  * Created by ngoehausen on 4/19/16.
  */
class DatasetDCFunctions[T](self: DC[T])(implicit tEncoder: Encoder[T]) {


//  def as[U : Encoder]: Dataset[U]

  def getDataset(sc: SparkContext): Dataset[T] ={
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext.createDataset(self.getRDD(sc))
  }

}
