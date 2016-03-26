package org.apress.prospark

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object CdrDataframeExamplesApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: CdrDataframeExamplesApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val sqlC = new SQLContext(ssc.sparkContext)
    import sqlC.implicits._

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        val cdrs = seqToCdr(rdd).toDF()

        cdrs.select("squareId", "timeInterval", "countryCode").show()
        cdrs.select($"squareId", $"timeInterval", $"countryCode").show()
        cdrs.filter("squareId = 5").show()
        cdrs.drop("countryCode").show()
        cdrs.select($"squareId", $"timeInterval", $"countryCode").where($"squareId" === 5).show()
        cdrs.limit(5).show()
        cdrs.groupBy("squareId").count().show()
        cdrs.groupBy("countryCode").avg("internetTrafficActivity").show()
        cdrs.groupBy("countryCode").max("callOutActivity").show()
        cdrs.groupBy("countryCode").min("callOutActivity").show()
        cdrs.groupBy("squareId").sum("internetTrafficActivity").show()
        cdrs.groupBy("squareId").agg(sum("callOutActivity"), sum("callInActivity"), sum("smsOutActivity"), sum("smsInActivity"), sum("internetTrafficActivity")).show()
        cdrs.groupBy("countryCode").sum("internetTrafficActivity").orderBy(desc("SUM(internetTrafficActivity)")).show()
        cdrs.agg(sum("callOutActivity"), sum("callInActivity"), sum("smsOutActivity"), sum("smsInActivity"), sum("internetTrafficActivity")).show()
        cdrs.rollup("squareId", "countryCode").count().orderBy(desc("squareId"), desc("countryCode")).rdd.saveAsTextFile("/tmp/rollup" + rdd.hashCode())
        cdrs.cube("squareId", "countryCode").count().orderBy(desc("squareId"), desc("countryCode")).rdd.saveAsTextFile("/tmp/cube" + rdd.hashCode())
        cdrs.dropDuplicates(Array("callOutActivity", "callInActivity")).show()
        cdrs.select("squareId", "countryCode", "internetTrafficActivity").distinct.show()
        cdrs.withColumn("endTime", cdrs("timeInterval") + 600000).show()
        cdrs.sample(true, 0.01).show()
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