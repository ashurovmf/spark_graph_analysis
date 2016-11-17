package com.gft.spark


import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by miav on 2016-10-28.
  */
object KDD99App {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark KDD 1999 Example")
      .getOrCreate()
    val kddSource =
      spark.sparkContext.textFile("C:\\Projects\\Bench\\Big_data_samples\\kdd99\\kddcup.data_10_percent.gz")
    val kddTable = kddSource.map( line => Vectors.dense(line.split(",").slice(4,12).map(_.toDouble))).cache()
    //find outliers in timeseries
//    val stats = kddTable.map( line => line(0)).stats()
//    val (mean, stddev) = (stats.mean, stats.stdev)
//    val outliers = kddTable.map(line => {
//      if(!(mean - 2 * stddev > line(0) && line(0) > mean + 2 * stddev))
//        line(0)
//    })
//    println(outliers.collect())

    //clustering by KMeans
    val numClusters = 5
    val clusters = KMeans.train(kddTable,numClusters,maxIterations = 10, runs = 1,initializationMode = "random")
//    val clusterSize = kddTable.map(line => clusters.predict(line)).countByValue()
    val dataDistance = kddTable.map(line=>{
      val cluster = clusters.predict(line)
      val centroid = clusters.clusterCenters(cluster)
      val dist = Vectors.sqdist(centroid,line)
      (dist,line)
    })
    val histogramDist = dataDistance.keys.histogram(10)
    //print histogram manualy
    for ( (length, count) <- (histogramDist._2 zip histogramDist._1))
      println(f"${length+1}%2s: ${count}")
  }

}
