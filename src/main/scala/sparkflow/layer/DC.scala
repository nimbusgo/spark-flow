package sparkflow.layer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.language.implicitConversions
import sparkflow._
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, Encoder}

/**
  * DistributedCollection, analogous to RDD
  */
abstract class DC[T: ClassTag](deps: Seq[Dependency[_]]) extends Dependency[T](deps) {

  private var _rdd: RDD[T] = _
  private var _dataset: Option[Dataset[T]] = None
  private var checkpointed = false

  protected def computeRDD(sc: SparkContext): RDD[T]
  protected def computeDataset(sc: SparkContext): Option[Dataset[T]]

  def getDataset(sc: SparkContext): Option[Dataset[T]] = {
    if (_dataset.isEmpty) {
      _dataset = computeDataset(sc)
    }
    _dataset
  }

  def getRDD(sc: SparkContext): RDD[T] = {
    if(_rdd == null){
      if (checkpointed){
        loadRDDCheckpoint[T](getHash, sc) match {
          case Some(existingRdd) => this._rdd = existingRdd
          case None =>
            this._rdd = computeRDD(sc)
            _rdd.cache()
            saveRDDCheckpoint(getHash, _rdd)
        }
      } else {
        this._rdd = this.computeRDD(sc)
      }
    }
    _rdd
  }

  def getDataFrame(sc: SparkContext): Option[DataFrame] = {
    getDataset(sc).map(_.toDF())
  }

  def map[U: ClassTag](f: T => U): DC[U] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.map(f), f)
  }

  def filter(f: T => Boolean): DC[T] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.filter(f), f)
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): DC[U] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.flatMap(f), f)
  }

  def zipWithUniqueId(): DC[(T, Long)] = {
    new RDDTransformDC(this, (rdd: RDD[T]) => rdd.zipWithUniqueId, "zipWithUniqueId")
  }

  def mapToResult[U:ClassTag](f: RDD[T] => U): DR[U] ={
    new DRImpl[T,U](this, f)
  }

  def mapWith[U:ClassTag, V:ClassTag](dr: DR[U])(f: (T,U) => V) = {
    new ResultDepDC(this, dr, f)
  }

  def checkpoint(): this.type = {
    this.checkpointed = true
    this
  }

}

object DC {

  implicit def dcToPairDCFunctions[K, V](dc: DC[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairDCFunctions[K, V] = {
    new PairDCFunctions(dc)
  }

  implicit def doubleDCToDoubleDCFunctions(dc: DC[Double]): DoubleDCFunctions = {
    new DoubleDCFunctions(dc)
  }

  implicit def dcToDatasetDCFunctions[T](dc: DC[T])(implicit tEncoder: Encoder[T]): DatasetDCFunctions[T] = {
    new DatasetDCFunctions(dc)
  }

}
