package org.apress.prospark

import scala.io.Source
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jvalue2extractable
import org.json4s.string2JsonInput

object CdrUDFApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: CdrUDFApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val sqlC = new SQLContext(ssc.sparkContext)
    import sqlC.implicits._

    def getCountryCodeMapping() = {
      implicit val formats = org.json4s.DefaultFormats
      parse(Source.fromURL("http://country.io/phone.json").mkString).extract[Map[String, String]].map(_.swap)
    }

    def getCountryNameMapping() = {
      implicit val formats = org.json4s.DefaultFormats
      parse(Source.fromURL("http://country.io/names.json").mkString).extract[Map[String, String]]
    }

    def getCountryName(mappingPhone: Map[String, String], mappingName: Map[String, String], code: Int) = {
      mappingName.getOrElse(mappingPhone.getOrElse(code.toString, "NotFound"), "NotFound")
    }

    val getCountryNamePartial = getCountryName(getCountryCodeMapping(), getCountryNameMapping(), _: Int)

    sqlC.udf.register("getCountryNamePartial", getCountryNamePartial)

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        val cdrs = seqToCdr(rdd).toDF()
        cdrs.registerTempTable("cdrs")

        sqlC.sql("SELECT getCountryNamePartial(countryCode) AS countryName, COUNT(countryCode) AS cCount FROM cdrs GROUP BY countryCode ORDER BY cCount DESC LIMIT 5").show()

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