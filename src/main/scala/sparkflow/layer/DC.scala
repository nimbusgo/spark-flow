package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.language.implicitConversions
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, Encoder}
import sparkflow._

/**
  * DistributedCollection, analogous to RDD
  */
abstract class DC[T: ClassTag](deps: Seq[Dependency[_]])(implicit tEncoder: Encoder[T]) extends Dependency[T](deps) {

  private var _dataset: Dataset[T] = _
  private var checkpointed = false

  protected def computeDataset(sc: SparkContext): Dataset[T]

  def getDataset(sc: SparkContext): Dataset[T] = {
    if (sparkflow.sQLContext == null){
      sparkflow.sQLContext = SQLContext.getOrCreate(sc)
    }
    if (_dataset == null){
      _dataset = computeDataset(sc)
    }
    _dataset
  }

  def getRDD(sc: SparkContext): RDD[T] = {
    getDataset(sc).rdd
  }

  def getDataFrame(sc: SparkContext): DataFrame = {
    getDataset(sc).toDF()
  }

  def map[U: ClassTag](f: T => U)(implicit uEncoder: Encoder[U]): DC[U] = {
    new TransformDC(this, (ds: Dataset[T]) => ds.map(f), f)
  }

  def filter(f: T => Boolean): DC[T] = {
    new TransformDC(this, (ds: Dataset[T]) => ds.filter(f), f)
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U])(implicit uEncoder: Encoder[U]): DC[U] = {
    new TransformDC(this, (ds: Dataset[T]) => ds.flatMap(f), f)
  }

  def zipWithUniqueId()(implicit tupleEncoder: Encoder[(T, Long)]): DC[(T,Long)] = {
    val f = (ds: Dataset[T]) => ds.rdd.zipWithUniqueId().toDS()
    new TransformDC(this, f, "zipWithUniqueId")
  }

  def mapRDDToResult[U:ClassTag](f: RDD[T] => U): DR[U] ={
    new DRImpl[T,U](this, f)
  }

  def mapDSToResult[U:ClassTag](f: Dataset[T] => U): DR[U] ={
    new DRImpl[T,U](this, f)
  }

  def mapWith[U:ClassTag, V:ClassTag](dr: DR[U])(f: (T,U) => V)(implicit vEncoder: Encoder[V]): DC[V] = {
    new ResultDepDC(this, dr, f)
  }

  def checkpoint(): this.type = {
    this.checkpointed = true
    this
  }

}

object DC {

  implicit def dcToPairDCFunctions[K, V](dc: DC[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null, tupleEncoder: Encoder[(K,V)]): PairDCFunctions[K, V] = {
    new PairDCFunctions(dc)
  }

  implicit def doubleDCToDoubleDCFunctions(dc: DC[Double]): DoubleDCFunctions = {
    new DoubleDCFunctions(dc)
  }


}
