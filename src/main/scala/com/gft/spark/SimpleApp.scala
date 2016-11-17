package com.gft.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by miav on 2016-10-26.
  */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "./build.sbt" // Should be some file on your system
    val conf = new SparkConf().setAppName("My Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}
