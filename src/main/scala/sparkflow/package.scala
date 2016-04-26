import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, SQLImplicits, SQLContext, Dataset, Encoder}
import sparkflow.layer.{SourceDC, ParallelCollectionDC, DC}
import java.io.File

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by ngoehausen on 3/24/16.
  */
package object sparkflow extends SQLImplicits with Serializable{


  private[sparkflow] var sQLContext: SQLContext = null
  protected override def _sqlContext: SQLContext = sQLContext

    // This must live here to preserve binary compatibility with Spark < 1.5.
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  val sentinelInt = -1

  def parallelize[T:ClassTag](seq: Seq[T])(implicit tEncoder: Encoder[T]): DC[T] = {
    new ParallelCollectionDC(seq)
  }

  def textFile(path: String,
               minPartitions: Int = sentinelInt) = {
    val sourceFunc = if(minPartitions == sentinelInt){
      (sc: SparkContext) => sc.textFile(path)
    } else {
      (sc: SparkContext) => sc.textFile(path, minPartitions)
    }
    new SourceDC[String](path, sourceFunc, "textFile")
  }

  def objectFile[T:ClassTag](path: String,
                             minPartitions: Int = sentinelInt)
                            (implicit tEncoder: Encoder[T])= {
    val sourceFunc = if(minPartitions == sentinelInt){
      (sc: SparkContext) => sc.objectFile[T](path)
    } else {
      (sc: SparkContext) => sc.objectFile[T](path, minPartitions)
    }
    new SourceDC[T](path, sourceFunc, "objectFile")
  }


  private var checkpointDir = "/tmp/sparkflow"
  def setCheckpointDir(s: String) = {checkpointDir = s}

  private[sparkflow] def saveRDDCheckpoint[T:ClassTag](hash: String, rdd: RDD[T]) = {
    rdd.saveAsObjectFile(new File(checkpointDir, hash).toString)
  }

  private[sparkflow] def loadRDDCheckpoint[T:ClassTag](hash: String, sc: SparkContext): Option[RDD[T]] = {
    Try{
      val attemptRDD = sc.objectFile[T](new File(checkpointDir, hash).toString)
      attemptRDD.first()
      attemptRDD
    }.toOption
  }

  implicit def rddFuncToDSFunc[T: ClassTag, U](rddF: (RDD[T])=> U)(implicit tEncoder: Encoder[T]): Dataset[T] => U = {
    (ds: Dataset[T]) => rddF(ds.rdd)
  }

  implicit def dsFuncToRDDFunc[T: ClassTag, U](dsF: (Dataset[T])=> U)(implicit tEncoder: Encoder[T]): RDD[T] => U = {
    (rdd: RDD[T]) => dsF(rdd.toDS())
  }
  
}
