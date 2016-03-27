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
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.ObjectPool

object MqttSinkAppE {

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
          val mqttSink = MqttSinkPool().borrowObject()
          par.foreach(message => mqttSink.publish(topic, new MqttMessage(message.getBytes(StandardCharsets.UTF_8))))
          MqttSinkPool().returnObject(mqttSink)
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}

object MqttSinkPool {
  val poolSize = 8
  val brokerUrl = "tcp://localhost:1883"
  val mqttPool = new GenericObjectPool[MqttClient](new MqttClientFactory(brokerUrl))
  mqttPool.setMaxTotal(poolSize)
  sys.addShutdownHook {
    mqttPool.close()
  }
  
  def apply(): GenericObjectPool[MqttClient] = {
    mqttPool
  }
}

class MqttClientFactory(brokerUrl: String) extends BasePooledObjectFactory[MqttClient] {
  override def create() = {
    val client = new MqttClient(brokerUrl, MqttClient.generateClientId(), new MemoryPersistence())
    client.connect()
    client
  }
  override def wrap(client: MqttClient) = new DefaultPooledObject[MqttClient](client)
  override def validateObject(pObj: PooledObject[MqttClient]) = pObj.getObject.isConnected()
  override def destroyObject(pObj: PooledObject[MqttClient]) = {
    pObj.getObject.disconnect()
    pObj.getObject.close()
  }
  override def passivateObject(pObj: PooledObject[MqttClient]) = {}
}
