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

object CdrDataframeExamples3App {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: CdrDataframeExamples3App <appname> <batchInterval> <hostname> <port> <gridJsonPath>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port, gridJsonPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val sqlC = new SQLContext(ssc.sparkContext)
    import sqlC.implicits._
    implicit val formats = DefaultFormats

    val gridFile = scala.io.Source.fromFile(gridJsonPath).mkString
    val gridGeo = (parse(gridFile) \ "features")
    val gridStr = gridGeo.children.map(r => {
      val c = (r \ "geometry" \ "coordinates").extract[List[List[List[Float]]]].flatten.flatten.map(r => JDouble(r))
      val l = List(("id", r \ "id"), ("x1", c(0)), ("y1", c(1)), ("x2", c(2)), ("y2", c(3)),
        ("x3", c(4)), ("y3", c(5)), ("x4", c(6)), ("y4", c(7)))
      compact(render(JObject(l)))
    })

    val gridDF = sqlC.read.json(ssc.sparkContext.makeRDD(gridStr))

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        val cdrs = seqToCdr(rdd).toDF()
        cdrs.join(gridDF, $"squareId" === $"id").show()
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