package hold

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Sandbox 
{
  def main(args: Array[String]) 
  {
    val conf = new SparkConf()
		.setMaster("local")
		.setAppName("Simple Application")
    val sc = new SparkContext(conf)

    // Turn a collection into an RDD
    val nums = sc.parallelize(List(1, 2, 3, 5, 8, 13))
    
    /*val mathFile = "/Users/jasonf/DevJava/workspaces/Spark/spark-sandbox/Posts.xml"
    val mathData = sc.textFile(mathFile, 2).cache()

    // Pass each element through a function
    val squares = nums.map(x => x*x)
    squares.saveAsTextFile("squares")
    
    // Keep elements passing a predicate
    val evens = squares.filter(x => x % 2 == 0)
    evens.saveAsTextFile("evens")
    
    // Map each element to zero or more others
    val sequence = nums.flatMap(x => Seq(x))
    sequence.saveAsTextFile("sequence")*/
    
    

/*	//val input = sc.parallelize(List(("coffee", 1) , ("coffee", 2) , ("panda", 4)))
	val input = sc.parallelize(List((1, "coffee"), (2, "coffee"), (2, "tea"), (3, "tea")))
	val result = input.combineByKey(
	  (v) => (v, 1),
	  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
	  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
	  ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
	  result.collectAsMap().map(println(_))

	val a = sc.parallelize (List("dog" ,"cat" ,"gnu" ,"salmon" ,"rabbit" ,"turkey" ,"wolf" ,"bear" ,"bee"))
	val b = sc.parallelize (List (5, 5, 2, 3, 2, 1, 4, 3, 1))
	val c = b.zip(a)
	c.toArray.foreach(println)
	
	val d = c.combineByKey(
	    List ( _ ) , 
	    ( x : List [ String ] , y : String ) => y :: x , 
	    ( x : List [ String ] , y : List [ String ]) => x ::: y )
	
	d.collect.foreach(println)*/
    
  }
}