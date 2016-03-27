package org.apress.prospark

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.json4s.DefaultFormats
import org.json4s.JField
import org.json4s.JsonAST.JObject
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

object MqttSinkAppC {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: MqttSinkApp <appname> <outputBrokerUrl> <topic>")
      System.exit(1)
    }

    val Seq(appName, outputBrokerUrl, topic) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val batchInterval = 10

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    HttpUtils.createStream(ssc, url = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22IBM,GOOG,MSFT,AAPL,FB,ORCL,YHOO,TWTR,LNKD,INTC%22)%0A%09%09&format=json&diagnostics=true&env=http%3A%2F%2Fdatatables.org%2Falltables.env",
      interval = batchInterval)
      .flatMap(rec => {
        val query = parse(rec) \ "query"
        ((query \ "results" \ "quote").children).map(rec => JObject(JField("Timestamp", query \ "created")).merge(rec))
      })
      .map(rec => {
        implicit val formats = DefaultFormats
        rec.children.map(f => f.extract[String]) mkString ","
      })
      .foreachRDD { rdd =>
        rdd.foreachPartition { par =>
          val client = new MqttClient(outputBrokerUrl, MqttClient.generateClientId(), new MemoryPersistence())
          client.connect()
          par.foreach(rec => client.publish(topic, new MqttMessage(rec.getBytes(StandardCharsets.UTF_8))))
          client.disconnect()
          client.close()
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}
