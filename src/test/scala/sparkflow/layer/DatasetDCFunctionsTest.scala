package sparkflow.layer

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest._
import sparkflow._
import sparkflow.serialization.SomeCaseClasses._

/**
  * Created by ngoehausen on 4/19/16.
  */
class DatasetDCFunctionsTest extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("basic"){
    val dc = parallelize(1 to 10).map(_.toDouble)
    val ds = dc.getDataset(sc)
    ds.collect().foreach(println)

  }


  test("nested") {

    // TODO: upgrade to spark 2.0 where this works

//
//    val addresses = Array(Address("abc", 123), Address("def", 456))
//    val person = Person("bill", addresses)
//
//    val sqlContext = SQLContext.getOrCreate(sc)
//
//    parallelize(addresses).getDataset(sc).collect().foreach(println)
  }

}
