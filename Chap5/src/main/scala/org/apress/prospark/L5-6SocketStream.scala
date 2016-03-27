package org.apress.prospark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream.PairDStreamFunctions

import java.util.Calendar

object TripByYearApp {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: TripByYearApp <appname> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(10))

    ssc.socketTextStream(hostname, port.toInt)
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