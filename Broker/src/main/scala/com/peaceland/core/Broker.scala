package com.tp.spark.core


import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.rdd._
import com.tp.spark.utils.TweetUtils
import com.tp.spark.utils.TweetUtils._

/**
 *  We still use the dataset with the 8198 reduced tweets. The data are reduced tweets as the example below:
 *
 *  {"id":"572692378957430785",
 *  "user":"Srkian_nishu :)",
 *  "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *  "place":"Orissa",
 *  "country":"India"}
 *
 *  We want to make some computations on the users:
 *  - find all the tweets by user
 *  - find how many tweets each user has
 */
object Ex1UserMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   * Load the data from the json file and return an RDD of Tweet
   */
  def loadData(): RDD[Tweet] = {
    // Create the spark configuration and spark context
    val conf = new SparkConf()
      .setAppName("User mining")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(conf)

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile).mapPartitions(TweetUtils.parseFromJson(_))
  }

  /**
   * For each user return all his tweets
   */
  def tweetsByUser(): RDD[(String, Iterable[Tweet])] = {
    //tweets par utilisateur, concaténer deux listes en scala
    loadData().groupBy(_.user)
  }

  /**
   * Compute the number of tweets by user
   */
  def tweetByUserNumber(): RDD[(String, Int)] = {
    //loadData().groupBy(_.id).count()

    loadData().map { tweet => (tweet.user, 1) }
      .reduceByKey(_ + _)
  }

  /**
   * Top 10 twitterers
   */
  def topTenTwitterers(): Array[(String, Int)] = {
    loadData().map { tweet => (tweet.user, 1) }
      .reduceByKey(_ + _).takeOrdered(10) {
      Ordering[ Int].reverse.on(_._2)
    }
  }
}