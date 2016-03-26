package org.apress.prospark

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TranslateApp {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: TranslateApp <appname> <book_path> <output_path> <language>")
      System.exit(1)
    }
    val Seq(appName, bookPath, outputPath, lang) = args.toSeq

    val dict = getDictionary(lang)

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val sc = new SparkContext(conf)
    val book = sc.textFile(bookPath)
    val translated = book.map(line => line.split("\\s+").map(word => dict.getOrElse(word, word)).mkString(" "))
    translated.saveAsTextFile(outputPath)
  }

  def getDictionary(lang: String): Map[String, String] = {
    if (!Set("German", "French", "Italian", "Spanish").contains(lang)) {
      System.err.println(
        "Unsupported language: %s".format(lang))
      System.exit(1)
    }
    val url = "http://www.june29.com/IDP/files/%s.txt".format(lang)
    println("Grabbing dictionary from: %s".format(url))
    Source.fromURL(url, "ISO-8859-1").mkString
      .split("\\r?\\n")
      .filter(line => !line.startsWith("#"))
      .map(line => line.split("\\t"))
      .map(tkns => (tkns(0).trim, tkns(1).trim)).toMap
  }

}