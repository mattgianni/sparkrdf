package org.gianni.sparkrdf

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Submit {
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkRDF")
  val sc: SparkContext = new SparkContext(conf)
  
  val fn = "file:///Users/mattg/Projects/Spark/sparkrdf/data/nyse.nt"
  
  def main(args: Array[String]): Unit = {
    
    println("Welcome to SparkRDF")
    
    println("\nConfiguration:")
    conf.getAll.foreach({ case (k, v) => println(k + " = " + v) })
    
    println("\nLoading data file: " + fn)
    val text = sc.textFile(fn)
    println("Number of lines: " + text.count)

    println("First line: " + text.first)
    
    println("Exitting.")
  }
}