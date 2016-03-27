package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.json4s.DefaultFormats
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

object StatefulUpdateStateApp {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: StatefulUpdateStateApp <appname> <checkpointDir>")
      System.exit(1)
    }

    val Seq(appName, checkpointDir) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))
    ssc.checkpoint(checkpointDir)

    HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        implicit val formats = DefaultFormats
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children)
          .map(rec => ((rec \ "symbol").extract[String], ((rec \ "LastTradePriceOnly").extract[String].toFloat, (rec \ "Volume").extract[String].toLong)))
      })
      .updateStateByKey(updateState)
      .print()

    def updateState(values: Seq[(Float, Long)], state: Option[(Long, Long, Long)]): Option[(Long, Long, Long)] = {
      val volumes = values.map(s => s._2)
      val localMin = volumes.min
      val localMax = volumes.max
      val localCount500 = values.map(s => s._1).count(price => price > 500)
      val globalValues = state.getOrElse((Long.MaxValue, Long.MinValue, 0L)).asInstanceOf[(Long, Long, Long)]
      val newMin = if (localMin < globalValues._1) localMin else globalValues._1
      val newMax = if (localMax > globalValues._2) localMax else globalValues._2
      val newCount500 = globalValues._3 + localCount500
      return Some(newMin, newMax, newCount500)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

