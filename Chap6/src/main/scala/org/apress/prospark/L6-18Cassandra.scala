package org.apress.prospark

import java.nio.charset.StandardCharsets
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats
import org.json4s.JField
import org.json4s.JsonAST.JObject
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import java.nio.ByteBuffer
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.cassandra.thrift.Mutation
import java.util.Arrays

object CassandraSinkApp {

  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println(
        "Usage: CassandraSinkApp <appname> <cassandraHost> <cassandraPort> <keyspace> <columnFamilyName> <columnName>")
      System.exit(1)
    }

    val Seq(appName, cassandraHost, cassandraPort, keyspace, columnFamilyName, columnName) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10
    val windowSize = 20
    val slideInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        implicit val formats = DefaultFormats
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children)
          .map(rec => ((rec \ "symbol").extract[String], (rec \ "LastTradePriceOnly").extract[String].toFloat))
      })
      .reduceByKeyAndWindow((x: Float, y: Float) => (x + y), Seconds(windowSize), Seconds(slideInterval))
      .foreachRDD(rdd => {
        val jobConf = new Configuration()
        ConfigHelper.setOutputRpcPort(jobConf, cassandraPort)
        ConfigHelper.setOutputInitialAddress(jobConf, cassandraHost)
        ConfigHelper.setOutputColumnFamily(jobConf, keyspace, columnFamilyName)
        ConfigHelper.setOutputPartitioner(jobConf, "Murmur3Partitioner")
        rdd.map(rec => {
          val c = new Column()
          c.setName(ByteBufferUtil.bytes(columnName))
          c.setValue(ByteBufferUtil.bytes(rec._2 / (windowSize / batchInterval)))
          c.setTimestamp(System.currentTimeMillis)
          val m = new Mutation()
          m.setColumn_or_supercolumn(new ColumnOrSuperColumn())
          m.column_or_supercolumn.setColumn(c)
          (ByteBufferUtil.bytes(rec._1), Arrays.asList(m))
        }).saveAsNewAPIHadoopFile(keyspace, classOf[ByteBuffer], classOf[List[Mutation]], classOf[ColumnFamilyOutputFormat], jobConf)
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

