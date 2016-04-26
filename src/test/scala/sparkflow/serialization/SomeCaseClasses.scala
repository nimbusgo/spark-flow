package sparkflow.serialization

/**
  * Created by ngoehausen on 4/21/16.
  */
object SomeCaseClasses {

  case class Address(street : String, housenumber : Int)

  case class Person(name: String, addresses : Array[Address])
}
