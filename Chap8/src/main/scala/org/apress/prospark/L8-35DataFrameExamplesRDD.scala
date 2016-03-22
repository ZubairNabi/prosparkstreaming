package org.apress.prospark

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats

object CdrDataframeExamplesRDDApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: CdrDataframeExamplesRDDApp <appname> <batchInterval> <hostname> <schemaPath>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port, schemaFile) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val sqlC = new SQLContext(ssc.sparkContext)
    import sqlC.implicits._
    implicit val formats = DefaultFormats

    val schemaJson = scala.io.Source.fromFile(schemaFile).mkString
    val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        val cdrs = seqToCdr(rdd).toDF()
        val highInternet = sqlC.createDataFrame(cdrs.rdd.filter(r => r.getFloat(3) + r.getFloat(4) >= r.getFloat(5) + r.getFloat(6)), schema)
        val highOther = cdrs.except(highInternet)
        val highInternetGrid = highInternet.select("squareId", "countryCode").dropDuplicates()
        val highOtherGrid = highOther.select("squareId", "countryCode").dropDuplicates()
        highOtherGrid.except(highInternetGrid).show()
        highInternetGrid.except(highOtherGrid).show()
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