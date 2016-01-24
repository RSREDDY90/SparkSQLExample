package com.spark.sql.ex.SparkSQLExample

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.functions._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
object SparkSaveToCassandra {
  
  val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.driver.allowMultipleContexts", "true")
      .setSparkHome("${SPARK_HOME}")

    val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rawInputFile = sc.textFile("D:/Workspace/scala/SparkSQLExample/input/customerinfo.txt")
    
    val bankRawInputFile = sc.textFile("D:/Workspace/scala/SparkSQLExample/input/customerList.txt")

    // Import Row.
    import org.apache.spark.sql.Row;

    // Import Spark SQL data types
    import org.apache.spark.sql.types.{ StructType, StructField, StringType };

    val inputFileColumns = "name,age,srno"
    val inputSchema = StructType(inputFileColumns.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val inputRDD = rawInputFile.map(_.split(" ")).map(p => Row(p(0), p(1).trim,p(2)))
    val inputDF = sqlContext.createDataFrame(inputRDD, inputSchema)
    
        
    val bankFileColumns = "name,age,location,srno"
    val bankSchema = StructType(bankFileColumns.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val bankRDD = bankRawInputFile.map(_.split(" ")).map(p => Row(p(0), p(1).trim,p(2),p(3)))
    val bankDF = sqlContext.createDataFrame(bankRDD, bankSchema)

    // Register the DataFrames as a table.
    inputDF.registerTempTable("people")
    bankDF.registerTempTable("peopleList")
    
    val joinResult = sqlContext.sql("SELECT people.srno, people.name, peopleList.location, peopleList.age FROM people JOIN peopleList ON people.name=peopleList.name ORDER BY people.age")
   //val rr = joinResult.map { x => (x(0),x(1),x(2)) }
    
    val dd  = joinResult.map { x => SparkSaveToCassandra.inserToCassandra(x(0).toString(),x(1).toString(),x(2).toString()) }
    dd.foreach { println }
    //rr.foreach(println)
    
  }
  
  def inserToCassandra(param1: String, param2: String, param3: String): String = {

      val c = CassandraConnector(sc.getConf)

       val inq : String = "INSERT INTO spark.person_details (id, name, location) VALUES ("+param1+",'"+param2+"','"+param3+"')"
      
     
     println("query is:"+inq)
     
     val list = c.withSessionDo ( session => session.execute(inq) )
     
     
    return "HI"

  }
  
  
}