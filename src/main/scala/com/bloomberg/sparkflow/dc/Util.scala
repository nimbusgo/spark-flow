package com.bloomberg.sparkflow.dc


import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by ngoehausen on 5/18/16.
  */
object Util {


  private[dc] def saveCheckpoint[T:ClassTag](checkpointPath: String, dataset: Dataset[T]) = {
    assert(dataset != null)
    dataset.write.mode(SaveMode.Overwrite).parquet(checkpointPath)
  }

  private[dc] def loadCheckpoint[T: ClassTag](checkpointPath: String, spark: SparkSession)(implicit tEncoder: Encoder[T]): Option[Dataset[T]] = {
    if (pathExists(checkpointPath, spark.sparkContext)) {
      Try {
        val dataset = spark.read.parquet(checkpointPath).as[T]
        dataset.count()
        dataset
      }.toOption
    } else {
      None
    }
  }

  def pathExists(dir: String, sc: SparkContext) = {
    val path = new Path(dir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.exists(path)
  }

  def deletePath(dir: String, sc: SparkContext) = {
    val path = new Path(dir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.delete(path, true)
  }

}
