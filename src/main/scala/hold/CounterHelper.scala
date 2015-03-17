package hold

import org.apache.spark.rdd.RDD

object CounterHelper {
  
  def lineLengths(rdd: RDD[String]): RDD[Int] = {
    return rdd.map(x => x.length())
  }
  
  def totalLength(rdd: RDD[Int]): Int = {
    return rdd.reduce((x,y) => x + y)
  }
  
  def myLengthFunction(x: String) : Int = {
    return x.length();
  }
  
  def myAccumlator(x: Int, y: Int) : Int = {
    return x + y
  }

}