package hold

import com.datastax.spark.connector.CassandraRow

object FilterHelper 
{
	def func1(row: CassandraRow, startDate: String, endDate: String): Boolean = 
	{
	  return true
	}
}