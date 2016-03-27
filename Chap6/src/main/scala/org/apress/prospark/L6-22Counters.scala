package org.apress.prospark

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

object StatefulCountersApp {

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println(
        "Usage: StatefulCountersApp <appname>")
      System.exit(1)
    }

    val Seq(appName) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))
    
    var globalMax: AtomicLong = new AtomicLong(Long.MinValue)
    var globalMin: AtomicLong = new AtomicLong(Long.MaxValue)
    var globalCounter500: AtomicLong = new AtomicLong(0)

    HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        implicit val formats = DefaultFormats
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children)
          .map(rec => ((rec \ "symbol").extract[String], (rec \ "LastTradePriceOnly").extract[String].toFloat, (rec \ "Volume").extract[String].toLong))
      })
      .foreachRDD(rdd => {
        val stocks = rdd.take(10)
        stocks.foreach(stock => {
          val price = stock._2
          val volume = stock._3
          if (volume > globalMax.get()) {
            globalMax.set(volume)
          }
          if (volume < globalMin.get()) {
            globalMin.set(volume)
          }
          if (price > 500) {
            globalCounter500.incrementAndGet()
          }
        })
        if (globalCounter500.get() > 1000L) {
          println("Global counter has reached 1000")
          println("Max ----> " + globalMax.get)
          println("Min ----> " + globalMin.get)
          globalCounter500.set(0)
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

