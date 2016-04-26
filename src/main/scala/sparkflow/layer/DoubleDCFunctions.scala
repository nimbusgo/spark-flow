package sparkflow.layer

/**
  * Created by ngoehausen on 4/19/16.
  */
class DoubleDCFunctions(self: DC[Double]) {

  def sum(): DR[Double] = {
    self.mapRDDToResult(rdd => rdd.sum)
  }

}
