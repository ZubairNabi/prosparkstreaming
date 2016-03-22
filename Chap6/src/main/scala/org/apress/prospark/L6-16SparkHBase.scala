package org.apress.prospark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
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

object SparkHBaseBulkPutApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: SparkHBaseBulkPutApp <appname> <tableName> <columnFamilyName> <columnName>")
      System.exit(1)
    }

    val Seq(appName, tableName, columnFamilyName, columnName) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10
    val windowSize = 20
    val slideInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    val hbaseConf = HBaseConfiguration.create()
    val hContext = new HBaseContext(ssc.sparkContext, hbaseConf)

    val windowed = HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        implicit val formats = DefaultFormats
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children)
          .map(rec => ((rec \ "symbol").extract[String], (rec \ "LastTradePriceOnly").extract[String].toFloat))
      })
      .reduceByKeyAndWindow((x: Float, y: Float) => (x + y), Seconds(windowSize), Seconds(slideInterval))

    hContext.streamBulkPut[(String, Float)](windowed, TableName.valueOf(tableName), rec => {
      val put = new Put(rec._1.getBytes)
      put.addColumn(columnFamilyName.getBytes, columnName.getBytes, Bytes.toBytes(rec._2 / (windowSize / batchInterval)))
      put
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

