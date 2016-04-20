package sparkflow.layer

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest._
import sparkflow._
import sparkflow.layer.DC._

/**
  * Created by ngoehausen on 4/19/16.
  */
class DatasetDCFunctionsTest extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("basic"){
//    val dc = parallelize(1 to 10).map(_.toDouble)
//
//    val res = dc.sum()
//    println(res.get(sc))
//

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
//    val ds = sqlContext.createDataset( 1 to 10)
//    ds.collect().foreach(println)


    val dc = parallelize(1 to 10).map(_.toDouble)
    val newDs = dc.getDataset(sc)

    newDs.toDF().write.parquet("/tmp/blah")

  }

}
