package com.scala.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.when
import scala.io.Source

object personal extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  Logger.getLogger("com.dataflowdeveloper.links").setLevel(Level.INFO)

  val warehouseLocation = "spark-warehouse"
  val spark = SparkSession
    .builder()
    .appName("SI_CDM")
    .master("local")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    //.enableHiveSupport()
    .getOrCreate()
  import spark.implicits._
  val df = Seq(
    (1, "First Value", java.sql.Date.valueOf("2010-01-01")),
    (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))).toDF("int_column", "string_column", "date_column")

  //df.show(false)
  val path = "C:/Users/Nidhi.Chaudhary/Desktop/abc.csv"
  val df1= spark.read.option("header", "true").csv(path)
  //df1.show(false)
  val df2= spark.read.option("header", "true").csv(path)

  
  val out_df = innerJoin(df1, df2)
  //out_df.show(false)
  
  def innerJoin(df1: DataFrame, df2: DataFrame): DataFrame = {
    val innerJoinDF = df1.join(df2, "b")
    innerJoinDF
  }
  df2.createOrReplaceTempView("df1")
  df2.show(false)

   val nullColSchema = df2.columns
   val colswithErrorMesage = nullColSchema.foldLeft(df2)((colswithNull, c) => 
      colswithNull.withColumn(s"$c"+"_error",when(df2.col(c).isNull, s"$c"+"_isNull|").otherwise(lit(""))))
  //val df3 = 
  colswithErrorMesage.show(false)
//  val filtered = colswithErrorMesage.select(nullColSchema.filter(colName => !nullColSchema.contains(colName)).map(colName=>new Column(colName)): _*)
  //df4.show(false)
//  filtered.show(false)
  val list = nullColSchema.foldLeft(colswithErrorMesage)((l,r) => l.drop(r))
  list.show(false)
  val result = list.withColumn("newCol", concat(list.columns.map(c => col(c)): _*))        
  result.show(false)
  val part = list.columns.foldLeft(result)((l,r) => l.drop(r))
  val df4 = df2.withColumn("id", monotonically_increasing_id())
  val df5 = part.withColumn("id", monotonically_increasing_id())
  val df6 = df4.join(df5, Seq("id"), "outer").drop("id")
  df6.show(false)
  val goodFile = df6.na.drop.show(false) //good file
  val badFile = df6.filter(!($"newCol"==="")).show(false)
}