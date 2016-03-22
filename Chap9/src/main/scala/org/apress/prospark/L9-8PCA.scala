package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object PCAApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: PCAApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val substream = ssc.socketTextStream(hostname, port.toInt)
      .filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")

    val datastream = substream.map(f => Array(f(1), f(4), f(5), f(6), f(20), f(21), f(22), f(36), f(37), f(38)))
      .map(f => f.map(v => v.toDouble))
      .map(f => LabeledPoint(f(0), Vectors.dense(f.slice(1, f.length))))

    datastream.foreachRDD(rdd => {
      val pca = new PCA(rdd.first().features.size / 2)
        .fit(rdd.map(_.features))
      val testTrain = rdd.randomSplit(Array(0.3, 0.7))
      val test = testTrain(0).map(lp => lp.copy(features = pca.transform(lp.features)))
      val train = testTrain(1).map(lp => lp.copy(features = pca.transform(lp.features)))
      train.take(20).foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}