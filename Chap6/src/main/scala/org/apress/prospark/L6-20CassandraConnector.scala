package org.apress.prospark

import scala.reflect.runtime.universe

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

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming.toDStreamFunctions
import com.datastax.spark.connector.toNamedColumnRef

object CassandraConnectorSinkApp {

  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println(
        "Usage: CassandraConnectorSinkApp <appname> <cassandraHost> <cassandraPort> <keyspace> <tableName> <columnName>")
      System.exit(1)
    }

    val Seq(appName, cassandraHost, cassandraPort, keyspace, tableName, columnName) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort)

    val batchInterval = 10
    val windowSize = 20
    val slideInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }".format(keyspace))
      session.execute(s"CREATE TABLE IF NOT EXISTS %s.%s (key TEXT PRIMARY KEY, %s FLOAT)".format(keyspace, tableName, columnName))
    }

    HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        implicit val formats = DefaultFormats
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children)
          .map(rec => ((rec \ "symbol").extract[String], (rec \ "LastTradePriceOnly").extract[String].toFloat))
      })
      .reduceByKeyAndWindow((x: Float, y: Float) => (x + y), Seconds(windowSize), Seconds(slideInterval))
      .map(stock => (stock._1, stock._2 / (windowSize / batchInterval)))
      .saveToCassandra(keyspace, tableName)

    ssc.start()
    ssc.awaitTermination()
  }
}

