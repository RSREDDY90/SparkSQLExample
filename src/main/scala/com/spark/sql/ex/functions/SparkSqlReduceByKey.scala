package com.spark.sql.ex.functions

import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import java.util.Date
object SparkSqlReduceByKey {
  
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

    val serverInputFile = sc.textFile("D:/Workspace/scala/SparkSQLExample/inputReduceByKey/server.txt")
    
    // Import Row.
    import org.apache.spark.sql.Row;

    // Import Spark SQL data types
    import org.apache.spark.sql.types.{ StructType, StructField, StringType };

    val server_Columns = "serno,product,cmpny,server,expdate"
    val server_Schema = StructType(server_Columns.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val server_RDD = serverInputFile.map(_.split(",")).map(p => Row(p(0), p(1).trim,p(2),p(3),p(4)))
    val server_DF = sqlContext.createDataFrame(server_RDD, server_Schema)
    

    // Register the DataFrames as a table.
    server_DF.registerTempTable("server")
    
    val joinResult = sqlContext.sql("SELECT serno,product,cmpny,server,expdate from server")
    //val mapRDD = joinResult.map { x => (x(0),x(4)) }.reduceByKey((a, b) => (a,b))
    
    val mapRDD = joinResult.map { x => (x(0),x(4)) }.reduceByKey{case (x, y) => (x,y)}
    
    
    mapRDD.foreach(println)
    
    val data = mapRDD.map{case( x , y) =>
      y match {
      case (date1,date2) => 
          
          val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
          val firstDate = format.parse(date1.toString())
          val secondDate = format.parse(date2.toString())
          val currDate = new Date()
          var isPreviousDate : Date = null;
          if(firstDate.before(secondDate)){
            isPreviousDate = secondDate
          }
          if(firstDate.after(secondDate)){
            isPreviousDate = firstDate
          }
          (x,format.format(isPreviousDate))
          
        
      case y: String => (x,y)
      case _ => "many"
   } 
    }
    
    data.foreach(println)
    
  }
  
  
}