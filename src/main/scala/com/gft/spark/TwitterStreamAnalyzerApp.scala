package com.gft.spark

import com.gft.spark.utils.SentimentAnalyzer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by miav on 2016-10-31.
  */
object TwitterStreamAnalyzerApp {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Twitter Analyzing Example")
      .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None)
    stream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
//        val outputRDD = rdd.filter(status => !status.getLang.equals(status.getUser.getLang)).map(status => {
//          (status.getUser.getName,status.getUser.getLang,status.getText,status.getLang)
//        })
//        outputRDD.coalesce(1).saveAsTextFile("./tweets/text"+time.milliseconds.toString)
        val outputTweets = rdd.map(status => {
          Map(
            "createdAt" -> status.getCreatedAt,
            "tweetId" -> status.getId,
            "text" -> status.getText,
            "source" -> status.getSource,
            "hashtags" -> status.getHashtagEntities.map(_.getText).mkString(","),
            "replyTweetId" -> status.getInReplyToStatusId,
            "geoLocation" -> status.getGeoLocation,
            "lang" -> status.getLang,
            "userId" -> status.getUser.getId,
            "sentiment" -> SentimentAnalyzer.detectSentiment(status.getText))
        })
        outputTweets.coalesce(1).saveAsTextFile("./tweets/texts_"+time.milliseconds.toString)
        val outputUsers = rdd.map(status => {
          Map(
            "userId" -> status.getUser.getId,
            "userName" -> status.getUser.getName,
            "geoLocation" -> status.getUser.getLocation,
            "followersCount" -> status.getUser.getFollowersCount,
            "friendsCount" -> status.getUser.getFriendsCount,
            "createdAt" -> status.getUser.getCreatedAt,
            "lang" -> status.getUser.getLang,
            "isGoeEnabled" -> status.getUser.isGeoEnabled)
        })
        outputUsers.coalesce(1).saveAsTextFile("./tweets/users_"+time.milliseconds.toString)
        val langSentiments = outputTweets
//          .filter( tweet => { tweet.get("geoLocation").isDefined })
          .map( tweet => (tweet.get("lang") match {
          case Some(x:String) => x
          case _ => "++"
        }, tweet.get("sentiment") match {
            case Some(x:Double) => x
            case _ => -1.0
          }))
          .reduceByKey((sent1, sent2) => ((sent1+sent2)/2))
        langSentiments.coalesce(1).saveAsTextFile("./tweets/lang_"+time.milliseconds.toString)
      }
    })
    //val tags = stream.flatMap(line => line.getHashtagEntities.map(_.getText))
    //tags.saveAsTextFiles("./twits/tags")
    ssc.start()
    ssc.awaitTermination()
  }

}
