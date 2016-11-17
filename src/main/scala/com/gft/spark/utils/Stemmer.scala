package com.gft.spark.utils

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import scala.collection.mutable.ArrayBuffer

/**
  * Created by miav on 2016-10-31.
  */
object Stemmer {
  def tokenizeString(stringIn:String): Array[String] = {
    val result = ArrayBuffer.empty[String]
    val tokenAnalyzer = new StandardAnalyzer()
    val tokenStream = tokenAnalyzer.tokenStream("tokens",stringIn)
    tokenStream.reset()
    while (tokenStream.incrementToken()){
      result+=tokenStream.addAttribute(classOf[CharTermAttribute]).toString
    }
    result.toArray
  }
}
