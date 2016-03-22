package org.apress.prospark

import java.util.Timer
import java.util.TimerTask

import scala.reflect.ClassTag

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaDStream.fromDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

class HttpInputDStream(
    @transient ssc_ : StreamingContext,
    storageLevel: StorageLevel,
    url: String,
    interval: Long) extends ReceiverInputDStream[String](ssc_) with Logging {

  def getReceiver(): Receiver[String] = {
    new HttpReceiver(storageLevel, url, interval)
  }
}

class HttpReceiver(
    storageLevel: StorageLevel,
    url: String,
    interval: Long) extends Receiver[String](storageLevel) with Logging {

  var httpClient: CloseableHttpClient = _
  var trigger: Timer = _

  def onStop() {
    httpClient.close()
    logInfo("Disconnected from Http Server")
  }

  def onStart() {
    httpClient = HttpClients.createDefault()
    trigger = new Timer()
    trigger.scheduleAtFixedRate(new TimerTask {
      def run() = doGet()
    }, 0, interval * 1000)

    logInfo("Http Receiver initiated")
  }

  def doGet() {
    logInfo("Fetching data from Http source")
    val response = httpClient.execute(new HttpGet(url))
    try {
      val content = EntityUtils.toString(response.getEntity())
      store(content)
    } catch {
      case e: Exception => restart("Error! Problems while connecting", e)
    } finally {
      response.close()
    }

  }

}

object HttpUtils {
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
    url: String,
    interval: Long): DStream[String] = {
    new HttpInputDStream(ssc, storageLevel, url, interval)
  }

  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    url: String,
    interval: Long): JavaDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, url, interval)
  }
}
