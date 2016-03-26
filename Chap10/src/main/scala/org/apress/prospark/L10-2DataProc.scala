package org.apress.prospark

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JNothing
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

object DataProcApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: DataProcApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    ssc.socketTextStream(hostname, port.toInt)
      .map(r => {
        implicit val formats = DefaultFormats
        parse(r)
      })
      .filter(jvalue => {
        jvalue \ "attributes" \ "Wi-Fi" != JNothing
      })
      .map(jvalue => {
        implicit val formats = DefaultFormats
        ((jvalue \ "attributes" \ "Wi-Fi").extract[String], (jvalue \ "stars").extract[Int])
      })
      .combineByKey(
        (v) => (v, 1),
        (accValue: (Int, Int), v) => (accValue._1 + v, accValue._2 + 1),
        (accCombine1: (Int, Int), accCombine2: (Int, Int)) => (accCombine1._1 + accCombine2._1, accCombine1._2 + accCombine2._2),
        new HashPartitioner(ssc.sparkContext.defaultParallelism))
      .map({ case (k, v) => (k, v._1 / v._2.toFloat) })
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

}