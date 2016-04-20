package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import sparkflow.serialization.Hashing

/**
  * Created by ngoehausen on 4/20/16.
  */
class DataFrameSourceDC(val source: String, val sourceFunc: SQLContext => DataFrame, val sourceType: String) extends DC[Row](Nil) {

  override def computeRDD(sc: SparkContext) = {
    val sQLContext = SQLContext.getOrCreate(sc)
    val df = sourceFunc(sQLContext)
    df.rdd
  }

  override def computeDataset(sc: SparkContext) = {
    val sQLContext = SQLContext.getOrCreate(sc)
    val df = sourceFunc(sQLContext)
    Some(df.as[Row])
  }

  override def computeHash() = {
    Hashing.hashString(s"$sourceType:$source")
  }


}
