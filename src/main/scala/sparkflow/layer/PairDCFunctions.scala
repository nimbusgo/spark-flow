package sparkflow.layer

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import sparkflow._

/**
  * Created by ngoehausen on 4/19/16.
  */
class PairDCFunctions[K,V](self: DC[(K,V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null, //kEncoder: Encoder[K], vEncoder: Encoder[V],
     tupleEncoder: Encoder[(K,V)]){

  def identity() = {
    val f = (ds: Dataset[(K,V)]) => ds.rdd.toDS()
  }

  def reduceByKey(func: (V, V) => V): DC[(K, V)] = {
    new TransformDC(self, (ds: Dataset[(K,V)]) => ds.rdd.reduceByKey(func).toDS(), func)
  }
//
  def join[W](other: DC[(K,W)])(implicit kvwEncoder: Encoder[(K, (V,W))]): DC[(K, (V, W))] = {
    val resultFunc = (datasets: Seq[Dataset[_ <: Product2[K, _]]]) => {
      val left = datasets(0).asInstanceOf[Dataset[(K,V)]].rdd
      val right = datasets(1).asInstanceOf[Dataset[(K,W)]].rdd
      left.join(right).toDS()
    }
    new MultiInputDC[(K, (V, W)), K](Seq(self, other), resultFunc)
  }

//
//  def cogroup[W](other: DC[(K,W)]): DC[(K, (Iterable[V], Iterable[W]))] = {
//    val resultFunc = (rdds: Seq[Dataset[_ <: Product2[K, _]]]) => {
//      val left = rdds(0).asInstanceOf[Dataset[(K,V)]].rdd
//      val right = rdds(1).asInstanceOf[Dataset[(K,W)]].rdd
//      left.cogroup(right).toDS()
//    }
//    new MultiInputDC[((K, (Iterable[V], Iterable[W]))), K](Seq(self, other), resultFunc)
//  }
//
//  def cogroup[W1, W2](other1: DC[(K,W1)], other2: DC[(K,W2)])
//  : DC[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = {
//    val resultFunc = (rdds: Seq[Dataset[_ <: Product2[K, _]]]) => {
//      val first = rdds(0).asInstanceOf[Dataset[(K,V)]].rdd
//      val second = rdds(1).asInstanceOf[Dataset[(K,W1)]].rdd
//      val third = rdds(2).asInstanceOf[Dataset[(K,W2)]].rdd
//      first.cogroup(second, third).toDS()
//    }
//    new MultiInputDC[((K, (Iterable[V], Iterable[W1], Iterable[W2]))), K](Seq(self, other1, other2), resultFunc)
//  }

}
