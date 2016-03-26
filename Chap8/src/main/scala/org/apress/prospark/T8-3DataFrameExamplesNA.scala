package org.apress.prospark

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats
import org.json4s.JDouble
import org.json4s.JObject
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.compact
import org.json4s.native.JsonMethods.parse
import org.json4s.native.JsonMethods.render
import org.json4s.string2JsonInput

object CdrDataframeExamplesNAApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: CdrDataframeExamplesNAApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val sqlC = new SQLContext(ssc.sparkContext)
    import sqlC.implicits._
    implicit val formats = DefaultFormats

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        val cdrs = seqToCdr(rdd).toDF()
        cdrs.na.drop("any").show()
        cdrs.na.fill(0, Array("squareId")).show()
        cdrs.na.replace("squareId", Map(0 -> 1)).show()
        println("Correlation: " + cdrs.stat.corr("smsOutActivity", "callOutActivity"))
        println("Covariance: " + cdrs.stat.cov("smsInActivity", "callInActivity"))
        cdrs.stat.crosstab("squareId", "countryCode").show()
        cdrs.stat.freqItems(Array("squareId", "countryCode"), 0.1).show()
        cdrs.stat.crosstab("callOutActivity", "callInActivity").show()
      })

    ssc.start()
    ssc.awaitTermination()
  }

  def seqToCdr(rdd: RDD[Array[String]]): RDD[Cdr] = {
    rdd.map(c => c.map(f => f match {
      case x if x.isEmpty() => "0"
      case x => x
    })).map(c => Cdr(c(0).toInt, c(1).toLong, c(2).toInt, c(3).toFloat,
      c(4).toFloat, c(5).toFloat, c(6).toFloat, c(7).toFloat))
  }
}