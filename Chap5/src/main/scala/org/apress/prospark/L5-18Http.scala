package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats
import org.json4s.JField
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

object HttpApp {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: HttpApp <master> <appname> <outputPath>" +
          " In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    val Seq(master, appName, outputPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    HttpUtils.createStream(ssc, url = "https://www.citibikenyc.com/stations/json", interval = batchInterval)
      .flatMap(rec => (parse(rec) \ "stationBeanList").children)
      .filter(rec => {
        implicit val formats = DefaultFormats
        (rec \ "statusKey").extract[Integer] != 1
      })
      .map(rec => rec.filterField {
        case JField("id", _) => true
        case JField("stationName", _) => true
        case JField("statusValue", _) => true
        case _ => false
      })
      .map(rec => {
        implicit val formats = DefaultFormats
        (rec(0)._2.extract[Integer], rec(1)._2.extract[String], rec(2)._2.extract[String])
      })
      .saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }

}