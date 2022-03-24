package com.pj.peaceland.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerRecord}
/**
 *  Here the goal is to count how much each word appears in a file and make some operation on the result.
 *  We use the mapreduce pattern to do this:
 *
 *  step 1, the mapper:
 *  - we attribute 1 to each word. And we obtain then couples (word, 1), where word is the key.
 *
 *  step 2, the reducer:
 *  - for each key (=word), the values are added and we will obtain the total amount.
 */
object Drone {
  /**
   *  Load the data from the text file and return an RDD of words
   */
  def loadData(): RDD[String] = {
    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure the common properties
    // like the app name, the master url, memories allocation...
    val pathToFile = "data/message.txt"
    case class message(emotion:String,behavior:String, pscore:Int, date:String)

    val conf = new SparkConf()
                        .setAppName("Drone")
                        .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    val sc = SparkContext.getOrCreate(conf)

    // load data and create an RDD where each element will be a word
    // Here the flatMap method is used to separate the word in each line using the space separator
    // In this way it returns an RDD where each "element" is a word
    sc.textFile(pathToFile)
      .flatMap(_.split(" "))
  }




  /**
   *  Now count how much each word appears!
   */
  def wordcount(): RDD[(String, Int)] = {
    val rdd = loadData();
    rdd.map(word => (word, 1)).reduceByKey(_ + _)
  }

  /**
   *  Now keep the word which appear strictly more than 4 times!
   */
  def filterOnWordcount(): RDD[(String, Int)] = {
    val rdd = wordcount();
    rdd.filter(_._2 > 4);
  }
}
