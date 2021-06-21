package SparkPack18

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObj18 {
  
   def main(args:Array[String]):Unit={
    
 val conf = new SparkConf().setAppName("First").setMaster("local[*]")
 val sc = new SparkContext(conf)
 sc.setLogLevel("ERROR")
 
 val spark = SparkSession.builder().getOrCreate()
 import spark.implicits._
 
 
 val ajsondf = spark.read.format("json").option("multiLine","true").load("file:///C://data//complex/array1.json")
  
  ajsondf.show(false)
  ajsondf.printSchema()
  
  val ardf = ajsondf.select(
      
     "Students",
     "address.Permanent_address",
     "address.temporary_address",
     "first_name",
     "second_name"
     )
     
  ardf.show(false)
  ardf.printSchema()
  
   val exdf = ardf.withColumn("Students",explode(col("Students")))
      exdf.show(false)
  exdf.printSchema()
  
  val fldf = exdf.select(
        col("Students.user.name.title").alias("Stu_title"),
        col("Students.user.name.first").alias("Stu_first_name")    ,
         col("Students.user.name.last").alias("Stu_last_name"),
         col("Students.user.gender"),
      col("Students.user.address.Permanent_address").alias("Stu_Permanent_address"),
      col("Students.user.address.temporary_address").alias("Stu_temporary_address"),
      
       col("first_name").alias("Org_first_name")    ,
        col("second_name").alias("Org_second_name"),
     
     
      col("Permanent_address").alias("Org_Permanent_address"),
        col("temporary_address").alias("Org_temporary_address")
       
      ) 
  
   fldf.show(false)
  fldf.printSchema()
   }
  
}