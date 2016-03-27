package org.apress.prospark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream.PairDStreamFunctions

import java.util.Calendar

object TripByYearMultiApp {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: TripByYearMultiApp <appname> <hostname> <base_port> <num_of_sockets>")
      System.exit(1)
    }
    val Seq(appName, hostname, basePort, nSockets) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(10))

    val streams = (0 to nSockets.toInt - 1).map(i => ssc.socketTextStream(hostname, basePort.toInt + i))
    val uniStream = ssc.union(streams)

    uniStream
      .map(rec => rec.split(","))
      .map(rec => (rec(13), rec(0).toInt))
      .reduceByKey(_ + _)
      .map(pair => (pair._2, normalizeYear(pair._1)))
      .transform(rec => rec.sortByKey(ascending = false))
      .saveAsTextFiles("TripByYear")

    ssc.start()
    ssc.awaitTermination()
  }

  def normalizeYear(s: String): String = {
    try {
      (Calendar.getInstance().get(Calendar.YEAR) - s.toInt).toString
    } catch {
      case e: Exception => s
    }
  }
}