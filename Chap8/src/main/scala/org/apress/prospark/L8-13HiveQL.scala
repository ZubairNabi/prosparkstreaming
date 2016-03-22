package org.apress.prospark

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object CdrHiveqlApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: CdrHiveqlApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val cl = Thread.currentThread().getContextClassLoader()
    val hiveC = new HiveContext(ssc.sparkContext)
    Thread.currentThread().setContextClassLoader(cl)

    import hiveC.implicits._

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        seqToCdr(rdd).toDF().registerTempTable("cdrs")

        hiveC.sql("SET DATE_FMT='yy-MM-dd|HH'")
        hiveC.sql("SELECT from_unixtime(timeInterval, ${hiveconf:DATE_FMT}) AS TS, SUM(smsInActivity + smsOutActivity + callInActivity + callOutActivity + internetTrafficActivity) AS Activity FROM cdrs GROUP BY from_unixtime(timeInterval, ${hiveconf:DATE_FMT}) ORDER BY Activity DESC").show()
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