package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object PreprocessingApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: PreprocessingAppApp <appname> <batchInterval> <hostname> <port>")
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

    substream.map(f => Array(f(2), f(4), f(5), f(6)))
      .map(f => f.map(v => v.toDouble))
      .map(f => Vectors.dense(f))
      .foreachRDD(rdd => {
        val scalerModel = new StandardScaler().fit(rdd)
        val scaledRDD = scalerModel.transform(rdd)
      })

    ssc.start()
    ssc.awaitTermination()
  }

}