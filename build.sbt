name := "spark_test"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.0",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"
)
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0"
libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "6.2.1"
libraryDependencies ++= Seq(
  "com.databricks" % "spark-xml_2.11" % "0.4.0",
  "com.databricks" % "spark-csv_2.11" % "1.5.0"
)
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.8"
libraryDependencies += "commons-net" % "commons-net" % "3.1"