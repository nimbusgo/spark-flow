package sparkflow.layer

import scala.reflect.ClassTag
import org.apache.spark.sql.{Dataset, SQLContext, Encoder}

/**
  * Created by ngoehausen on 4/20/16.
  */
class DatasetDC[T:ClassTag](prev: DC[T])(implicit tEncoder: Encoder[T]) extends DC[T](Nil){

}
