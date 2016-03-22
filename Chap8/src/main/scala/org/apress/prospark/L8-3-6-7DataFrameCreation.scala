package org.apress.prospark

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.desc
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.native.Serialization.write
import org.json4s.DefaultFormats

object DataframeCreationApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: CdrDataframeApp <appname> <batchInterval> <hostname> <port>")
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
        //val cdrs = sqlC.createDataFrame(seqToCdr(rdd))
        //val cdrs = sqlC.createDataFrame(seqToCdr(rdd).collect())
        //val cdrs = seqToCdr(rdd).toDF()
        val cdrsJson = seqToCdr(rdd).map(r => {
          implicit val formats = DefaultFormats
          write(r)
        })
        val cdrs = sqlC.read.json(cdrsJson)

        cdrs.groupBy("countryCode").count().orderBy(desc("count")).show(5)
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