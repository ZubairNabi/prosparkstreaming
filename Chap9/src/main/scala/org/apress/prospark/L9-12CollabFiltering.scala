package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object CollabFilteringApp {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: CollabFilteringApp <appname> <batchInterval> <iPath>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, iPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val ratingStream = ssc.textFileStream(iPath).map(_.split(" ") match {
      case Array(subject, activity, freq) =>
        Rating(subject.toInt, activity.toInt, freq.toDouble)
    })

    val rank = 10
    val numIterations = 10
    val lambda = 0.01
    ratingStream.foreachRDD(ratingRDD => {
      val testTrain = ratingRDD.randomSplit(Array(0.3, 0.7))
      val model = ALS.train(testTrain(1), rank, numIterations, lambda)
      val test = testTrain(0).map {
        case Rating(subject, activity, freq) =>
          (subject, activity)
      }
      val prediction = model.predict(test)
      prediction.take(5).map(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}