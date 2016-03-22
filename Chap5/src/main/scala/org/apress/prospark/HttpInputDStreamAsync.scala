package org.apress.prospark

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaDStream.fromDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import com.ning.http.client.AsyncCompletionHandler
import com.ning.http.client.AsyncHttpClient
import com.ning.http.client.Response

class HttpInputDStreamAsync(
    @transient ssc_ : StreamingContext,
    storageLevel: StorageLevel,
    url: String) extends ReceiverInputDStream[String](ssc_) with Logging {

  def getReceiver(): Receiver[String] = {
    new HttpReceiverAsync(storageLevel, url)
  }
}

class HttpReceiverAsync(
    storageLevel: StorageLevel,
    url: String) extends Receiver[String](storageLevel) with Logging {

  var asyncHttpClient: AsyncHttpClient = _

  def onStop() {
    asyncHttpClient.close()
    logInfo("Disconnected from Http Server")
  }

  def onStart() {
    asyncHttpClient = new AsyncHttpClient()
    asyncHttpClient.prepareGet(url).execute(new AsyncCompletionHandler[Response]() {

      override def onCompleted(response: Response): Response = {
        store(response.getResponseBody)
        return response
      }

      override def onThrowable(t: Throwable) {
        restart("Error! Problems while connecting", t)
      }
    });
    logInfo("Http Connection initiated")
  }
  
}

object HttpUtilsAsync {
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
    url: String): DStream[String] = {
    new HttpInputDStreamAsync(ssc, storageLevel, url)
  }

  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    url: String): JavaDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, url)
  }
}
