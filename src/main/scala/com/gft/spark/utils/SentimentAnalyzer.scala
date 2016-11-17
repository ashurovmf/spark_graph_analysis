package com.gft.spark.utils

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by miav on 2016-11-02.
  */
object SentimentAnalyzer {

  val pipeline = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    val pipeline = new StanfordCoreNLP(props)
    pipeline
  }

  def detectSentiment(message: String): Double = {

    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    for (sentence <- sentences) {
      val tree = sentence.asInstanceOf[CoreMap].get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.asInstanceOf[CoreMap].toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length

//      println("debug: " + sentiment)
//      println("size: " + partText.length)

    }
    val averageSentiment: Double = {
      if (sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if (sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }

    /*
     0 -> very negative
     1 -> negative
     2 -> neutral
     3 -> positive
     4 -> very positive
     */
    averageSentiment
  }

}
