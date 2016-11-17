package com.gft.spark


import com.gft.spark.utils.Stemmer
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession

/**
  * Created by miav on 2016-10-28.
  */
object TextProcessingApp {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Text Analyzing Example")
      .getOrCreate()
    val sample = "Flick. A tiny, almost invisible movement, and the house was still."
    val tokens = Stemmer.tokenizeString(sample)
    //hashing our tokens
    val tf = new HashingTF(10)
    val hashed = tf.transform(tokens)
  }
}
