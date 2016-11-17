package com.gft.spark

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Created by miav on 2016-10-26.
  */
object MusicApp {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Music Example")
      .getOrCreate()
    val trackFile = spark.sparkContext.textFile("C:\\Projects\\Bench\\spark_test\\tracks.csv")
    val tkv_by_customer = trackFile.map(line => {
      val cols = line.split(",").map(_.trim)
      //customerId => trackId,DateTime,MobileBool,ListeningZipBool
      ({cols(1)},Array(Array({cols(2)},{cols(3)},{cols(4)},{cols(5)})))
    }).reduceByKey((a,b)=> a ++ b)
    val custdata = tkv_by_customer.mapValues(userTracks => {
      var trackList = ListBuffer.empty[Int]
      var (mob, mon, aft, eve, nigh) = (0,0,0,0,0)
      for (track <- userTracks){
        val hourL = track(1).split(" ")(1).split(":")(0)
        val trackId = track(0)

        if(!trackList.contains(trackId)){
          trackList.+=(trackId.toInt)
        }
        mob += track(2).toInt
        if(hourL.toInt < 5){
          nigh += 1
        } else if (hourL.toInt < 12) {
          mon += 1
        } else if (hourL.toInt < 17) {
          aft += 1
        } else if (hourL.toInt < 22) {
          eve += 1
        } else {
          nigh += 1
        }
      }
      (trackList.length,mon,aft,eve,nigh,mob)
    })
    val monAvg = custdata.map(userDetails => (1,(userDetails._2._2, 1)))
      .reduceByKey((u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
      .mapValues(line => line._1.toDouble/line._2.toDouble)
      .first()._2
//    val aftAvg = custdata.map(userDetails => userDetails._2._3).sum() / custdata.map(userDetails => userDetails._2._3).count()
//    val eveAvg = custdata.map(userDetails => userDetails._2._4).sum() / custdata.map(userDetails => userDetails._2._4).count()
//    val nighAvg = custdata.map(userDetails => userDetails._2._5).sum() / custdata.map(userDetails => userDetails._2._5).count()
    println(monAvg)
//    custdata.coalesce(1,true).saveAsTextFile("C:\\Projects\\Bench\\spark_test\\result.csv")
    spark.stop()
  }

}
