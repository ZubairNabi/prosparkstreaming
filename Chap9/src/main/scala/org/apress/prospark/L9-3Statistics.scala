package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object StatisticsApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: StatisticsApp <appname> <batchInterval> <hostname> <port>")
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
      .map(f => f.map(f => f.toDouble))

    substream.map(f => Vectors.dense(f.slice(1, 5))).foreachRDD(rdd => {
      val stats = Statistics.colStats(rdd)
      println("Count: " + stats.count)
      println("Max: " + stats.max.toArray.mkString(" "))
      println("Min: " + stats.min.toArray.mkString(" "))
      println("Mean: " + stats.mean.toArray.mkString(" "))
      println("L1-Norm: " + stats.normL1.toArray.mkString(" "))
      println("L2-Norm: " + stats.normL2.toArray.mkString(" "))
      println("Number of non-zeros: " + stats.numNonzeros.toArray.mkString(" "))
      println("Varience: " + stats.variance.toArray.mkString(" "))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}