package com.harold.bigdata.twitter

import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.spark.streaming.twitter.TwitterUtils
import com.harold.bigdata.utils.Utilities.{setupTwitter,setupLogging}

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    println("Inicio del programa")
    setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val homeDir = System.getProperty("user.home")

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())

    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    // Now eliminate anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))

    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    // Print the top 10
    println("####Popular Hashtags####")
    sortedResults.print

    // Print out the first ten
    println("####Twitter message####")
    statuses.print()

    // Keep count of how many Tweets we're received so we can stop automatically
    var totalTweets: Long = 0

    statuses.foreachRDD((rdd, time) => {
      if(rdd.count > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD =  rdd.repartition(1).cache
        // Print out a directory with the results
        repartitionedRDD.saveAsTextFile(homeDir+"/Tweets_"+time.milliseconds.toString)
        // Stop once we've collected 100 tweets.
        totalTweets += repartitionedRDD.count()
        println("####Twitter count####")
        println("Tweet count: " + totalTweets)
        if(totalTweets > 100) {
          System.exit(0)
        }
      }
    })

// Set a checkpoint directory, and kick it all off
    ssc.checkpoint(homeDir+"/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}