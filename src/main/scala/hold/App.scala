package hold

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.sql._

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import org.apache.spark.sql._

/**
 * @author ${user.name}
 */
object SimpleApp {

  
  def main(args: Array[String]) 
  {		
	val conf = new SparkConf(true)
		.setMaster("local")
		.setAppName("Simple Application")
    	.set("spark.cassandra.connection.host", "ohp-bi-test")

    val sc = new SparkContext(conf)

	/*val mathFile = "/Users/jasonf/DevJava/workspaces/Spark/spark-sandbox/Posts.xml"
    val mathData = sc.textFile(mathFile, 2).cache()
    val chainRuleData = mathData
    	.filter(line => line.contains("chain"))
    	.filter(line => line.contains("rule"))
    val numChainRule = chainRuleData.count()
    println("Chain and rule occurrances: %s".format(numChainRule))
    
    // Word count, the hello world of big data applications!
    val words = mathData.flatMap(x => x.split(" "))
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)   
    println("Word Count in math file: %s".format(result.count))
    result.toArray.foreach(println)*/
    

	/*val lines = sc.textFile("/Users/jasonf/Spark/usb/spark/README.md")
	val lineLengths = lines.map(x => x.length()).reduce((x,y) => x + y)
	println(lineLengths)
	
	val lengths2 = lines.map(x => CounterHelper.myLengthFunction(x)).reduce((x,y) => CounterHelper.myAccumlator(x, y))
	println(lengths2)
	
	val total = CounterHelper.totalLength(CounterHelper.lineLengths(lines));
    println(total);*/
    
	//val lines = sc.textFile("/Users/jasonf/Spark/usb/spark/README.md")
	//val vals = sc.parallelize(List(List(1, 2), List(3, 4), List(5, 6)))
	//val test = vals.flatMap(x => x)
	//test.foreach(println)
    
    val data1 = List(
        ("a","a", 1L, 1.0), 
        ("a","a", 2L, 2.0), 
        ("a","b", 3L, 3.0), 
        ("a","b", 4L, 4.0), 
        ("b","a", 1L, 5.0), 
        ("b","a", 2L, 6.0), 
        ("b","b", 3L, 7.0), 
        ("b","b", 4L, 8.0),
        ("c","a", 1L, 9.0), 
        ("c","a", 2L, 10.0), 
        ("c","b", 3L, 11.0), 
        ("c","b", 4L, 12.0))
    val arr = sc.parallelize(data1)
    val temp2 = arr.map(row => ((row._1, row._2), (row._3, row._4)))
    	.combineByKey(
    	    (v: (Long, Double)) => (v), 
    	    (acc: (Long, Double), v: (Long, Double)) => (if (acc._1 >= v._1) acc else v),
    	    (acc1: (Long, Double), acc2: (Long, Double)) => (if (acc1._1 >= acc2._1) acc1 else acc2))
    	.map(tuple => (tuple._1._1, tuple._1._2, tuple._2._1, tuple._2._2))

	// Cache a few tables from the ads keyspace for later use
	val encounters = sc.cassandraTable("ads", "encounter_registry").cache()
	
	
	//val adsV = sc.cassandraTable("ads", "ads_registry_v").cache()
	
	// Compute two years of milliseconds
	val twoYearsMillis: Long = 2L * 365 * 24 * 60 * 60 * 1000
	
	// Here we get the ids for patients with 2 or more encounters in prior (rolling) two
	// years and keep them for later use
	val twoPlusEncounters = encounters
    	.select("patient_id", "encounter_start_date")
		.filter(row => row.getLong("encounter_start_date") >= (new java.util.Date().getTime() - twoYearsMillis))
    	.map(row => (row.getString("patient_id"), 1))
    	.reduceByKey((x,y) => x + y)
    	.filter(row => (row._2 >= 2))
    	.map(tuple => tuple._1)
    	
    val v1 = sc.parallelize(List(("65768-0114","LDLC"),(1384239600000L,6.0),
        ("101 019 3800","LDLC"),(1384239600000L,""),
        ("2040608","LDLC"),(1384239600000L,3.1)))
    val v2 = sc.parallelize(List(("65768-0114","HBA1C"),(1384239600000L,4.9)))
    
    

  }
  

}