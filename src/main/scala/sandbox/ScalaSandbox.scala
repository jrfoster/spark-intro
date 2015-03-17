package sandbox

object ScalaSandbox {

  def main(args: Array[String]): Unit = {
    case class Person(
        name: String,
        age: Int,
        gender: String)
        
    
    val somePeople: Option[Person] = Some(Person("Devyn",30,"Female"))
    val nonePeople: Option[Person] = None;
    
    val someDouble: Option[Double] = Some(0.0D)
    val noneDouble: Option[Double] = None
    
    println(somePeople.map{_.name}.getOrElse(null))
    println(nonePeople.map{_.name}.getOrElse(null))

    println(someDouble.getOrElse(None))
    println(noneDouble.getOrElse(None))
  
  }

}