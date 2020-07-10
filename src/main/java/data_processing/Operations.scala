package data_processing

import scala.collection.mutable
import org.apache.spark.rdd._
import org.apache.spark._
import scala.collection.immutable._
import scala.reflect.ClassTag
import org.apache.kafka.clients.consumer._
import scala.concurrent._

object Operations {

  //Use 2 cores for processing the data
  val sparkConfig : SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[2]")
  val sparkContext : SparkContext = new SparkContext(sparkConfig)

  def tokeniseText(text : String, isLowercase : Boolean) : Map[String,Int] = {
    implicit val classTag : ClassTag[String] = ClassTag.apply(classOf[String])
    var wordMap : HashMap[String,Int] = new HashMap[String,Int]()
    val wordRDD : RDD[String] = sparkContext.makeRDD(text.split("\\s"))
      .filter(_.r.pattern.eq("[a-zA-Z0-9]".r.pattern))
      .map(word => if(isLowercase) word.toLowerCase else word)

      wordRDD.foreach(word => {
        if (wordMap.contains(word)){
          val occurrenceCount : Int = wordMap.get(word).get + 1
          wordMap = wordMap - word
          wordMap = wordMap + ((word, occurrenceCount))
        }
        else wordMap + ((word, 1))
      })
    wordMap
  }

  def readFromKafka(topic : String): Unit ={
    val future: Future[analysistext.AnalysisText]
  }
}
