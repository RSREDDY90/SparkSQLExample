package com.spark.sql.ex.SparkSQLExample
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.functions._

object SparkSqlIntro1 {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[2]", "sqq", System.getenv("SPARK_HOME"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rawInputFile = sc.textFile("D:/Workspace/scala/SparkSQLExample/input/customerinfo.txt")
    
    val bankRawInputFile = sc.textFile("D:/Workspace/scala/SparkSQLExample/input/customerList.txt")

    // Import Row.
    import org.apache.spark.sql.Row;

    // Import Spark SQL data types
    import org.apache.spark.sql.types.{ StructType, StructField, StringType };

    val inputFileColumns = "name,age"
    val inputSchema = StructType(inputFileColumns.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val inputRDD = rawInputFile.map(_.split(" ")).map(p => Row(p(0), p(1).trim))
    val inputDF = sqlContext.createDataFrame(inputRDD, inputSchema)
    
        
    val bankFileColumns = "name,age,location"
    val bankSchema = StructType(bankFileColumns.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val bankRDD = bankRawInputFile.map(_.split(" ")).map(p => Row(p(0), p(1).trim,p(2)))
    val bankDF = sqlContext.createDataFrame(bankRDD, bankSchema)

    // Register the DataFrames as a table.
    inputDF.registerTempTable("people")
    bankDF.registerTempTable("peopleList")
    
    val joinResult = sqlContext.sql("SELECT people.name, peopleList.location, peopleList.age FROM people JOIN peopleList ON people.name=peopleList.name ORDER BY people.age")
    val rr = joinResult.map { x => (x(0),x(1),x(2)) }
    rr.foreach(println)
    rr.saveAsTextFile("output")
   // joinResult.map { x => (x(0),x(1),(2)) }.saveAsTextFile("output")
    
  }

}
