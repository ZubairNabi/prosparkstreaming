package org.apress.prospark

import scala.collection.mutable

import org.apache.spark.AccumulableParam
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

object StatefulAccumulatorsApp {

  object StockAccum extends AccumulableParam[mutable.HashMap[String, (Long, Long, Long)], (String, (Float, Long))] {
    def zero(t: mutable.HashMap[String, (Long, Long, Long)]): mutable.HashMap[String, (Long, Long, Long)] = {
      new mutable.HashMap[String, (Long, Long, Long)]()
    }
    def addInPlace(t1: mutable.HashMap[String, (Long, Long, Long)], t2: mutable.HashMap[String, (Long, Long, Long)]): mutable.HashMap[String, (Long, Long, Long)] = {
      t1 ++ t2.map {
        case (k, v2) => (k -> {
          val v1 = t1.getOrElse(k, (Long.MaxValue, Long.MinValue, 0L))
          val newMin = if (v2._1 < v1._1) v2._1 else v1._1
          val newMax = if (v2._2 > v1._2) v2._2 else v1._2
          (newMin, newMax, v1._3 + v2._3)
        })
      }
    }
    def addAccumulator(t1: mutable.HashMap[String, (Long, Long, Long)], t2: (String, (Float, Long))): mutable.HashMap[String, (Long, Long, Long)] = {
      val prevStats = t1.getOrElse(t2._1, (Long.MaxValue, Long.MinValue, 0L))
      val newVals = t2._2
      var newCount = prevStats._3
      if (newVals._1 > 500.0) {
        newCount += 1
      }
      val newMin = if (newVals._2 < prevStats._1) newVals._2 else prevStats._1
      val newMax = if (newVals._2 > prevStats._2) newVals._2 else prevStats._2
      t1 += t2._1 -> (newMin, newMax, newCount)
    }
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: StatefulAccumulatorsApp <appname> <checkpointDir>")
      System.exit(1)
    }

    val Seq(appName, checkpointDir) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    val stateAccum = ssc.sparkContext.accumulable(new mutable.HashMap[String, (Long, Long, Long)]())(StockAccum)

    HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        implicit val formats = DefaultFormats
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children)
          .map(rec => ((rec \ "symbol").extract[String], ((rec \ "LastTradePriceOnly").extract[String].toFloat, (rec \ "Volume").extract[String].toLong)))
      })
      .foreachRDD(rdd => {
        rdd.foreach({ stock =>
          stateAccum += (stock._1, (stock._2._1, stock._2._2))
        })
        for ((sym, stats) <- stateAccum.value.to) printf("Symbol: %s, Stats: %s\n", sym, stats)
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

