package hold

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext.rddToPairRDDFunctions


import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions

object ADSRegistrySandbox {
  
  def main(args: Array[String]) {
	
    val conf = new SparkConf(true)
		.setMaster("local")
		.setAppName("Simple Application")
    	.set("spark.cassandra.connection.host", "ohp-bi-test")
    	
    val sc = new SparkContext(conf)

    // This base transformation will pull out the most recent "measure" from the vertical measure table
    // It includes the 4 pieces of data that describe that most recent measure
    val adsV = sc.cassandraTable("ads", "ads_registry_v").cache()
    val mostRecentV = adsV
    	.select("patient_id", "measure_name", "measure_date", "measure_value")
    	.filter(row => row.getAny("measure_date") != null && row.getAny("measure_value") != null)
    	.map(row => ((row.getString("patient_id"), row.getString("measure_name")), (row.getLong("measure_date"), row.getDouble("measure_value"))))
    	.combineByKey(
    	    (v: (Long, Double)) => (v), 
    	    (acc: (Long, Double), v: (Long, Double)) => (if (acc._1 >= v._1) acc else v),
    	    (acc1: (Long, Double), acc2: (Long, Double)) => (if (acc1._1 >= acc2._1) acc1 else acc2))
    	.map(tuple => (tuple._1._1, tuple._1._2, tuple._2._1, tuple._2._2))
   
    // This base transformation will pull out the most recent measures that are known at compile time
    // from the horizontal measure table.  Since it cannot be known ahead of time what measures are 
    // stored in the table, each piece must be pulled out individually and combined at the end
    val adsH = sc.cassandraTable("ads", "ads_registry_h").cache()
    val mostRecentHBA1C = adsH
    	.select("patient_id", "hba1c_date", "hba1c_value")
        .filter(row => row.getAny("hba1c_date") != null && row.getAny("hba1c_value") != null)
    	.map(row => ((row.getString("patient_id"), "HBA1C"), (row.getLong("hba1c_date"), row.getDouble("hba1c_value"))))
    	.combineByKey(
    	    (v: (Long, Double)) => (v), 
    	    (acc: (Long, Double), v: (Long, Double)) => (if (acc._1 >= v._1) acc else v),
    	    (acc1: (Long, Double), acc2: (Long, Double)) => (if (acc1._1 >= acc2._1) acc1 else acc2))
    
    // This pattern would have to be repeated for each measure we store this way, 
    val mostRecentLDLC = adsH
    	.select("patient_id", "ldlc_date", "ldlc_value")
        .filter(row => row.getAny("ldlc_date") != null && row.getAny("ldlc_value") != null)
    	.map(row => ((row.getString("patient_id"), "LDLC"), (row.getLong("ldlc_date"), row.getDouble("ldlc_value"))))
    	.combineByKey(
    	    (v: (Long, Double)) => (v), 
    	    (acc: (Long, Double), v: (Long, Double)) => (if (acc._1 >= v._1) acc else v),
    	    (acc1: (Long, Double), acc2: (Long, Double)) => (if (acc1._1 >= acc2._1) acc1 else acc2))
    
    // Union together all intermediate results and build out a "flattened" structure for storage	
    val mostRecentH = mostRecentHBA1C
    	.union(mostRecentLDLC)
     	.map(tuple => (tuple._1._1, tuple._1._2, tuple._2._1, tuple._2._2))
  }

}