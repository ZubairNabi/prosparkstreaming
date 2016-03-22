package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object KMeansClusteringApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: KMeansClusteringApp <appname> <batchInterval> <hostname> <port>")
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

    val orientationStream = substream
      .map(f => Seq(1, 4, 5, 6, 10, 11, 12, 20, 21, 22, 26, 27, 28, 36, 37, 38, 42, 43, 44).map(i => f(i)).toArray)
      .map(arr => arr.map(_.toDouble))
      .filter(f => f(0) == 1.0 || f(0) == 2.0 || f(0) == 3.0)
      .map(f => LabeledPoint(f(0), Vectors.dense(f.slice(1, f.length))))
    val test = orientationStream.transform(rdd => rdd.randomSplit(Array(0.3, 0.7))(0))
    val train = orientationStream.transformWith(test, (r1: RDD[LabeledPoint], r2: RDD[LabeledPoint]) => r1.subtract(r2)).cache()
    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(0)
      .setRandomCenters(18, 0.0)

    model.trainOn(train.map(v => v.features))
    val prediction = model.predictOnValues(test.map(v => (v.label, v.features)))

    ssc.start()
    ssc.awaitTermination()
  }

}