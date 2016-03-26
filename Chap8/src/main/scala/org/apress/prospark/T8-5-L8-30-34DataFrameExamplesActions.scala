package org.apress.prospark

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apress.prospark.CdrDataframeExamplesActionsApp.Cdr
import org.json4s.DefaultFormats

object CdrDataframeExamplesActionsApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: CdrDataframeExamplesActionsApp <appname> <batchInterval> <hostname> <port>")
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
    implicit val formats = DefaultFormats

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        val cdrs = seqToCdr(rdd).toDF()

        val counts = cdrs.groupBy("countryCode").count().orderBy(desc("count"))
        counts.show(5)
        counts.show()
        println("head(5): " + counts.head(5))
        println("take(5): " + counts.take(5))
        println("head(): " + counts.head())
        println("first(5): " + counts.first())
        println("count(): " + counts.count())
        println("collect(): " + counts.collect())
        println("collectAsList(): " + counts.collectAsList())
        println("describe(): " + cdrs.describe("smsInActivity", "smsOutActivity", "callInActivity", "callOutActivity", "internetTrafficActivity").show())
        counts.write.format("parquet").save("/tmp/parquent" + rdd.id)
        counts.write.format("json").save("/tmp/json" + rdd.id)
        counts.write.parquet("/tmp/parquent2" + rdd.id)
        counts.write.json("/tmp/json2" + rdd.id)
        counts.write.saveAsTable("count_table")
        cdrs.groupBy("countryCode").count().orderBy(desc("count")).write.mode(SaveMode.Append).save("/tmp/counts")
        val prop: java.util.Properties = new java.util.Properties()
        counts.write.jdbc("jdbc:mysql://hostname:port/cdrsdb", "count_table", prop)
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