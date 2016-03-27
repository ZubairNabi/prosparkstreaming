package org.apress.prospark

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

import redis.clients.jedis.Jedis

object StatefulRedisApp {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: StatefulRedisApp <appname> <checkpointDir> <hostname>")
      System.exit(1)
    }

    val Seq(appName, checkpointDir, hostname) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        implicit val formats = DefaultFormats
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children)
          .map(rec => ((rec \ "symbol").extract[String], ((rec \ "LastTradePriceOnly").extract[String].toFloat, (rec \ "Volume").extract[String].toLong)))
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition({ part =>
          val jedis = new Jedis(hostname)
          part.foreach(f => {
            val prev = jedis.hmget(f._1, "min", "max", "count")
            if (prev(0) == null) {
              jedis.hmset(f._1, mutable.HashMap("min" -> Long.MaxValue.toString, "max" -> Long.MinValue.toString, "count" -> 0.toString))
            } else {
              val prevLong = prev.toList.map(v => v.toLong)
              var newCount = prevLong(2)
              val newPrice = f._2._1
              val newVolume = f._2._2
              if (newPrice > 500.0) {
                newCount += 1
              }
              val newMin = if (newVolume < prevLong(0)) newVolume else prevLong(0)
              val newMax = if (newVolume > prevLong(1)) newVolume else prevLong(1)
              jedis.hmset(f._1, mutable.HashMap("min" -> newMin.toString, "max" -> newMax.toString, "count" -> newCount.toString))
            }
          })
          jedis.close()
        })

        val jedis = new Jedis(hostname)
        jedis.scan(0).getResult.foreach(sym => println("Symbol: %s, Stats: %s".format(sym, jedis.hmget(sym, "min", "max", "count").toString)))
        jedis.close()
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

