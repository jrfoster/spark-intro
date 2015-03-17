package sandbox

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions

object CassandraTestApp 
{
  def main(args: Array[String]) 
  {
    val conf = new SparkConf(true)
    	.set("spark.cassandra.connection.host", "127.0.0.1")
    	
    val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)
    
    // Running in eclipse we need to tell Spark where our code is
    sc.addJar("/Users/jasonf/DevJava/workspaces/Spark/spark-sandbox/target/spark-sandbox-0.0.1-SNAPSHOT.jar")
    
    // Get the rows that have an abnormal hemoglobin and an abnormal sodium
    val results = sc.cassandraTable("pjug", "pat_results").cache()
    val abnormalHGB = results
    	.filter(x => x.getString("test_name") == "HGB")
    	.filter(y => y.getDouble("test_value") < 12)
    
    val abnormalNA = results
    	.filter(x => x.getString("test_name") == "NA")
    	.filter(y => y.getDouble("test_value") < 135)
    	
    // Here we are combining all our abnormal results into one RDD and then create
    // a key-value RDD based on patient id, assigning one point for each abnormal
    // result
    val abnormals = abnormalNA
    	.union(abnormalHGB)
    	.map(row => (row.getString("patient_id"), 1))
    	
    // Get those patients with a long length of stay and then create a key-value
    // RDD based on patient id, assigning two points for the long length of stay
    val encounters = sc.cassandraTable("pjug", "pat_encounters").cache()
    val longStay = encounters
    	.filter(x => x.getInt("length_of_stay") >= 5)
    	.map(row => (row.getString("patient_id"), 2))
    
    // Get those patients whose admissions were not elective and then create a
    // key-value RDD based on patient id, assigning one point for the non-elective
    // admission
    val nonElective = encounters
    	.filter(x => x.getString("admission_type") != "Elective" )
    	.map(row => (row.getString("patient_id"), 1))
    	
    // Get those patients who were admitted between 1 and 5 times (inclusive) and
    // assign them 2 points
    val admit1to5 = encounters
    	.select("patient_id")
    	.map(row => (row.getString("patient_id"), 1))
    	.reduceByKey((x,y) => x + y)
    	.filter(row => (row._2 >= 1 && row._2 <= 5))
    	.map(x => (x._1, 2))
    	
    // Get those patients who were admitted more than 5 times and assign them 5 points
    val admit5orGt = encounters
    	.select("patient_id")
    	.map(row => (row.getString("patient_id"), 1))
    	.reduceByKey((x,y) => x + y)
    	.filter(row => (row._2 > 5))
    	.map(x => (x._1, 5))
    	
    // This is a simple list of all patients so that we can be sure that all ids in
    // the population are represented
    val patients = sc.cassandraTable("pjug", "patient")
    	.map(row => (row.getString("patient_id"), 0))

    // Here we combine all our scoring data into one big RDD and then reduce that
    // RDD by key, collapsing and adding up the assigned scores for each occurrence
    // of patient id 
    val scoring = patients
    	.union(abnormals)
    	.union(longStay)
    	.union(nonElective)
    	.union(admit1to5)
    	.union(admit5orGt)
    	.reduceByKey((x,y) => x + y)
    	
	// Saving the individual patient readmission risk data to Cassandra
    scoring.saveToCassandra("pjug", "pat_readmission_risk")
  }
  
  def holdCode()
  {
    /*
    // This produces some rudimentary readmission risk stratification data for the population
    val strat = scoring
    	.map(x => (x._2, x._1))
    	.combineByKey(
		    List(_), 
		    (x : List[String], y : String) => y :: x, 
		    (x : List[String], y : List[String]) => x ::: y)
		.map(x => (x._1, x._2.size))
    
	// Saving the population readmission risk stratification data to Cassandra
	strat.saveToCassandra("pjug", "readmission_stratification")
	

	
	// Based on the stratification data, calculate our single metric
	val m1 = strat
		.filter(x => (x._1 > 13))
		.values
		.reduce((x, y) => x + y)
    	
	// Save the metrics to Cassandra.  
	val monikers = sc.parallelize(List("TOTAL_PATIENTS", "READMISSION_RISK_GT_13"))
	val values = sc.parallelize(List(patients.count, m1))
	val metrics = monikers.zip(values)
	
	// Saving the metrics to Cassandra
	metrics.saveToCassandra("pjug", "metrics")
	*/  
  }
}